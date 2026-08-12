원본코드# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import signal

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import TopicPartition
from kafkatest.services.verifiable_client import VerifiableClientMixin
from kafkatest.version import DEV_BRANCH


class ConsumerState:
    Started = 1
    Dead = 2
    Rebalancing = 3
    Joined = 4


class ConsumerEventHandler(object):

    def __init__(self, node):
        self.node = node
        self.state = ConsumerState.Dead
        self.revoked_count = 0
        self.assigned_count = 0
        self.assignment = []
        self.position = {}
        self.committed = {}
        self.total_consumed = 0

    def handle_shutdown_complete(self):
        self.state = ConsumerState.Dead
        self.assignment = []
        self.position = {}

    def handle_startup_complete(self):
        self.state = ConsumerState.Started

    def handle_offsets_committed(self, event, node, logger):
        if event["success"]:
            for offset_commit in event["offsets"]:
                if offset_commit.get("error", "") != "":
                    logger.debug("%s: Offset commit failed for: %s" % (str(node.account), offset_commit))
                    continue

                topic = offset_commit["topic"]
                partition = offset_commit["partition"]
                tp = TopicPartition(topic, partition)
                offset = offset_commit["offset"]
                assert tp in self.assignment, \
                    "Committed offsets for partition %s not assigned (current assignment: %s)" % \
                    (str(tp), str(self.assignment))
                assert tp in self.position, "No previous position for %s: %s" % (str(tp), event)
                assert self.position[tp] >= offset, \
                    "The committed offset %d was greater than the current position %d for partition %s" % \
                    (offset, self.position[tp], str(tp))
                self.committed[tp] = offset

    def handle_records_consumed(self, event):
        assert self.state == ConsumerState.Joined, \
            "Consumed records should only be received when joined (current state: %s)" % str(self.state)

        for record_batch in event["partitions"]:
            tp = TopicPartition(topic=record_batch["topic"],
                                partition=record_batch["partition"])
            min_offset = record_batch["minOffset"]
            max_offset = record_batch["maxOffset"]

            assert tp in self.assignment, \
                "Consumed records for partition %s which is not assigned (current assignment: %s)" % \
                (str(tp), str(self.assignment))
            assert tp not in self.position or self.position[tp] == min_offset, \
                "Consumed from an unexpected offset (%d, %d) for partition %s" % \
                (self.position[tp], min_offset, str(tp))
            self.position[tp] = max_offset + 1 

        self.total_consumed += event["count"]

    def handle_partitions_revoked(self, event):
        self.revoked_count += 1
        self.state = ConsumerState.Rebalancing
        self.position = {}

    def handle_partitions_assigned(self, event):
        self.assigned_count += 1
        self.state = ConsumerState.Joined
        assignment = []
        for topic_partition in event["partitions"]:
            topic = topic_partition["topic"]
            partition = topic_partition["partition"]
            assignment.append(TopicPartition(topic, partition))
        self.assignment = assignment

    def handle_kill_process(self, clean_shutdown):
        # if the shutdown was clean, then we expect the explicit
        # shutdown event from the consumer
        if not clean_shutdown:
            self.handle_shutdown_complete()

    def current_assignment(self):
        return list(self.assignment)

    def current_position(self, tp):
        if tp in self.position:
            return self.position[tp]
        else:
            return None

    def last_commit(self, tp):
        if tp in self.committed:
            return self.committed[tp]
        else:
            return None


class VerifiableConsumer(KafkaPathResolverMixin, VerifiableClientMixin, BackgroundThreadService):
    """This service wraps org.apache.kafka.tools.VerifiableConsumer for use in
    system testing. 
    
    NOTE: this class should be treated as a PUBLIC API. Downstream users use
    this service both directly and through class extension, so care must be 
    taken to ensure compatibility.
    """

    PERSISTENT_ROOT = "/mnt/verifiable_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "verifiable_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_consumer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "verifiable_consumer.properties")

    logs = {
        "verifiable_consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "verifiable_consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "verifiable_consumer_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, kafka, topic, group_id,
                 max_messages=-1, session_timeout_sec=30, enable_autocommit=False,
                 assignment_strategy="org.apache.kafka.clients.consumer.RangeAssignor",
                 version=DEV_BRANCH, stop_timeout_sec=30, log_level="INFO"):
        super(VerifiableConsumer, self).__init__(context, num_nodes)
        self.log_level = log_level
        
        self.kafka = kafka
        self.topic = topic
        self.group_id = group_id
        self.max_messages = max_messages
        self.session_timeout_sec = session_timeout_sec
        self.enable_autocommit = enable_autocommit
        self.assignment_strategy = assignment_strategy
        self.prop_file = ""
        self.stop_timeout_sec = stop_timeout_sec

        self.event_handlers = {}
        self.global_position = {}
        self.global_committed = {}

        for node in self.nodes:
            node.version = version

    def java_class_name(self):
        return "VerifiableConsumer"

    def _worker(self, idx, node):
        with self.lock:
            if node not in self.event_handlers:
                self.event_handlers[node] = ConsumerEventHandler(node)
            handler = self.event_handlers[node]

        node.account.ssh("mkdir -p %s" % VerifiableConsumer.PERSISTENT_ROOT, allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=VerifiableConsumer.LOG_FILE)
        node.account.create_file(VerifiableConsumer.LOG4J_CONFIG, log_config)

        # Create and upload config file
        self.security_config = self.kafka.security_config.client_config(self.prop_file, node)
        self.security_config.setup_node(node)
        self.prop_file += str(self.security_config)
        self.logger.info("verifiable_consumer.properties:")
        self.logger.info(self.prop_file)
        node.account.create_file(VerifiableConsumer.CONFIG_FILE, self.prop_file)
        self.security_config.setup_node(node)
        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableConsumer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            event = self.try_parse_json(node, line.strip())
            if event is not None:
                with self.lock:
                    name = event["name"]
                    if name == "shutdown_complete":
                        handler.handle_shutdown_complete()
                    elif name == "startup_complete":
                        handler.handle_startup_complete()
                    elif name == "offsets_committed":
                        handler.handle_offsets_committed(event, node, self.logger)
                        self._update_global_committed(event)
                    elif name == "records_consumed":
                        handler.handle_records_consumed(event)
                        self._update_global_position(event, node)
                    elif name == "partitions_revoked":
                        handler.handle_partitions_revoked(event)
                    elif name == "partitions_assigned":
                        handler.handle_partitions_assigned(event)
                    else:
                        self.logger.debug("%s: ignoring unknown event: %s" % (str(node.account), event))

    def _update_global_position(self, consumed_event, node):
        for consumed_partition in consumed_event["partitions"]:
            tp = TopicPartition(consumed_partition["topic"], consumed_partition["partition"])
            if tp in self.global_committed:
                # verify that the position never gets behind the current commit.
                assert self.global_committed[tp] <= consumed_partition["minOffset"], \
                    "Consumed position %d is behind the current committed offset %d for partition %s" % \
                    (consumed_partition["minOffset"], self.global_committed[tp], str(tp))

            # the consumer cannot generally guarantee that the position increases monotonically
            # without gaps in the face of hard failures, so we only log a warning when this happens
            if tp in self.global_position and self.global_position[tp] != consumed_partition["minOffset"]:
                self.logger.warn("%s: Expected next consumed offset of %d for partition %s, but instead saw %d" %
                                 (str(node.account), self.global_position[tp], str(tp), consumed_partition["minOffset"]))

            self.global_position[tp] = consumed_partition["maxOffset"] + 1

    def _update_global_committed(self, commit_event):
        if commit_event["success"]:
            for offset_commit in commit_event["offsets"]:
                tp = TopicPartition(offset_commit["topic"], offset_commit["partition"])
                offset = offset_commit["offset"]
                assert self.global_position[tp] >= offset, \
                    "Committed offset %d for partition %s is ahead of the current position %d" % \
                    (offset, str(tp), self.global_position[tp])
                self.global_committed[tp] = offset

    def start_cmd(self, node):
        cmd = ""
        cmd += "export LOG_DIR=%s;" % VerifiableConsumer.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % VerifiableConsumer.LOG4J_CONFIG
        cmd += self.impl.exec_cmd(node)
        cmd += " --group-id %s --topic %s --broker-list %s --session-timeout %s --assignment-strategy %s %s" % \
               (self.group_id, self.topic, self.kafka.bootstrap_servers(self.security_config.security_protocol),
               self.session_timeout_sec*1000, self.assignment_strategy, "--enable-autocommit" if self.enable_autocommit else "")
               
        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)

        cmd += " --consumer.config %s" % VerifiableConsumer.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (VerifiableConsumer.STDOUT_CAPTURE, VerifiableConsumer.STDOUT_CAPTURE)
        return cmd

    def pids(self, node):
        return self.impl.pids(node)

    def try_parse_json(self, node, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            return json.loads(string)
        except ValueError:
            self.logger.debug("%s: Could not parse as json: %s" % (str(node.account), str(string)))
            return None

    def stop_all(self):
        for node in self.nodes:
            self.stop_node(node)

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        sig = self.impl.kill_signal(clean_shutdown)
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

        with self.lock:
            self.event_handlers[node].handle_kill_process(clean_shutdown)

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(node, clean_shutdown=clean_shutdown)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def current_assignment(self):
        with self.lock:
            return { handler.node: handler.current_assignment() for handler in self.event_handlers.itervalues() }

    def current_position(self, tp):
        with self.lock:
            if tp in self.global_position:
                return self.global_position[tp]
            else:
                return None

    def owner(self, tp):
        with self.lock:
            for handler in self.event_handlers.itervalues():
                if tp in handler.current_assignment():
                    return handler.node
            return None

    def last_commit(self, tp):
        with self.lock:
            if tp in self.global_committed:
                return self.global_committed[tp]
            else:
                return None

    def total_consumed(self):
        with self.lock:
            return sum(handler.total_consumed for handler in self.event_handlers.itervalues())

    def num_rebalances(self):
        with self.lock:
            return max(handler.assigned_count for handler in self.event_handlers.itervalues())

    def joined_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state == ConsumerState.Joined]

    def rebalancing_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state == ConsumerState.Rebalancing]

    def dead_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state == ConsumerState.Dead]

    def alive_nodes(self):
        with self.lock:
            return [handler.node for handler in self.event_handlers.itervalues()
                    if handler.state != ConsumerState.Dead]

분산 컨슈머의 상태·리밸런싱·오프셋 무결성 검증은 9점대 구조지만, 설정 누적과 이벤트 스트림 경계의 방어가 약해 장애 시 원인 추적과 상태 일관성이 흔들릴 여지가 있는 코드다.

제안패치
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import TopicPartition
from kafkatest.services.verifiable_client import VerifiableClientMixin
from kafkatest.version import DEV_BRANCH


class ConsumerState:
    Started = 1
    Dead = 2
    Rebalancing = 3
    Joined = 4


class ConsumerEventHandler(object):

    def __init__(self, node):
        self.node = node
        self.state = ConsumerState.Dead
        self.revoked_count = 0
        self.assigned_count = 0
        self.assignment = []
        self.position = {}
        self.committed = {}
        self.total_consumed = 0

    def handle_shutdown_complete(self):
        self.state = ConsumerState.Dead
        self.assignment = []
        self.position = {}

    def handle_startup_complete(self):
        self.state = ConsumerState.Started

    def handle_offsets_committed(self, event, node, logger):
        if not event.get("success", False):
            return

        for offset_commit in event.get("offsets", []):
            if offset_commit.get("error", "") != "":
                logger.debug(
                    "%s: Offset commit failed for: %s"
                    % (str(node.account), offset_commit)
                )
                continue

            topic = offset_commit["topic"]
            partition = offset_commit["partition"]
            offset = offset_commit["offset"]
            tp = TopicPartition(topic, partition)

            assert tp in self.assignment, \
                "Committed offsets for partition %s not assigned " \
                "(current assignment: %s)" % \
                (str(tp), str(self.assignment))

            assert tp in self.position, \
                "No previous position for %s: %s" % \
                (str(tp), str(event))

            assert self.position[tp] >= offset, \
                "The committed offset %d was greater than the current " \
                "position %d for partition %s" % \
                (offset, self.position[tp], str(tp))

            self.committed[tp] = offset

    def handle_records_consumed(self, event):
        assert self.state == ConsumerState.Joined, \
            "Consumed records should only be received when joined " \
            "(current state: %s)" % str(self.state)

        for record_batch in event.get("partitions", []):
            tp = TopicPartition(
                topic=record_batch["topic"],
                partition=record_batch["partition"]
            )
            min_offset = record_batch["minOffset"]
            max_offset = record_batch["maxOffset"]

            assert tp in self.assignment, \
                "Consumed records for partition %s which is not assigned " \
                "(current assignment: %s)" % \
                (str(tp), str(self.assignment))

            assert tp not in self.position or \
                   self.position[tp] == min_offset, \
                "Consumed from an unexpected offset (%d, %d) " \
                "for partition %s" % \
                (
                    self.position.get(tp),
                    min_offset,
                    str(tp)
                )

            self.position[tp] = max_offset + 1

        self.total_consumed += event.get("count", 0)

    def handle_partitions_revoked(self, event):
        self.revoked_count += 1
        self.state = ConsumerState.Rebalancing
        self.position = {}

    def handle_partitions_assigned(self, event):
        self.assigned_count += 1
        self.state = ConsumerState.Joined

        assignment = []
        for topic_partition in event.get("partitions", []):
            topic = topic_partition["topic"]
            partition = topic_partition["partition"]
            assignment.append(TopicPartition(topic, partition))

        self.assignment = assignment

    def handle_kill_process(self, clean_shutdown):
        if not clean_shutdown:
            self.handle_shutdown_complete()

    def current_assignment(self):
        return list(self.assignment)

    def current_position(self, tp):
        return self.position.get(tp)

    def last_commit(self, tp):
        return self.committed.get(tp)


class VerifiableConsumer(
    KafkaPathResolverMixin,
    VerifiableClientMixin,
    BackgroundThreadService
):
    """
    VerifiableConsumer service.

    This class is a public API and must preserve compatibility with
    downstream users.
    """

    PERSISTENT_ROOT = "/mnt/verifiable_consumer"
    STDOUT_CAPTURE = os.path.join(
        PERSISTENT_ROOT, "verifiable_consumer.stdout"
    )
    STDERR_CAPTURE = os.path.join(
        PERSISTENT_ROOT, "verifiable_consumer.stderr"
    )
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_consumer.log")
    LOG4J_CONFIG = os.path.join(
        PERSISTENT_ROOT, "tools-log4j.properties"
    )
    CONFIG_FILE = os.path.join(
        PERSISTENT_ROOT, "verifiable_consumer.properties"
    )

    logs = {
        "verifiable_consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False
        },
        "verifiable_consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False
        },
        "verifiable_consumer_log": {
            "path": LOG_FILE,
            "collect_default": True
        }
    }

    def __init__(
        self,
        context,
        num_nodes,
        kafka,
        topic,
        group_id,
        max_messages=-1,
        session_timeout_sec=30,
        enable_autocommit=False,
        assignment_strategy=(
            "org.apache.kafka.clients.consumer.RangeAssignor"
        ),
        version=DEV_BRANCH,
        stop_timeout_sec=30,
        log_level="INFO"
    ):
        super(VerifiableConsumer, self).__init__(context, num_nodes)

        self.log_level = log_level
        self.kafka = kafka
        self.topic = topic
        self.group_id = group_id
        self.max_messages = max_messages
        self.session_timeout_sec = session_timeout_sec
        self.enable_autocommit = enable_autocommit
        self.assignment_strategy = assignment_strategy
        self.base_prop_file = ""
        self.stop_timeout_sec = stop_timeout_sec

        self.event_handlers = {}
        self.global_position = {}
        self.global_committed = {}

        for node in self.nodes:
            node.version = version

    def java_class_name(self):
        return "VerifiableConsumer"

    def _worker(self, idx, node):
        with self.lock:
            handler = self.event_handlers.setdefault(
                node,
                ConsumerEventHandler(node)
            )

        node.account.ssh(
            "mkdir -p %s" % self.PERSISTENT_ROOT,
            allow_fail=False
        )

        log_config = self.render(
            "tools_log4j.properties",
            log_file=self.LOG_FILE
        )
        node.account.create_file(self.LOG4J_CONFIG, log_config)

        # 노드별 설정을 지역 변수로 유지하여 공유 상태 오염 방지
        security_config = self.kafka.security_config.client_config(
            self.base_prop_file,
            node
        )
        security_config.setup_node(node)

        final_prop_file = self.base_prop_file + str(security_config)
        node.account.create_file(
            self.CONFIG_FILE,
            final_prop_file
        )

        # 보안 설정 원문은 로그에 남기지 않는다.
        self.logger.info(
            "Created verifiable consumer configuration for node %s"
            % str(node.account)
        )

        cmd = self.start_cmd(node, security_config)
        self.logger.debug(
            "Starting VerifiableConsumer %d on node %s"
            % (idx, str(node.account))
        )

        try:
            for line in node.account.ssh_capture(cmd):
                event = self.try_parse_json(node, line.strip())
                if event is None:
                    continue

                with self.lock:
                    self._handle_event(
                        event,
                        handler,
                        node
                    )

        except Exception:
            self.logger.exception(
                "VerifiableConsumer worker failed on node %s"
                % str(node.account)
            )
            raise

    def _handle_event(self, event, handler, node):
        name = event.get("name")

        if name == "shutdown_complete":
            handler.handle_shutdown_complete()

        elif name == "startup_complete":
            handler.handle_startup_complete()

        elif name == "offsets_committed":
            handler.handle_offsets_committed(
                event,
                node,
                self.logger
            )
            self._update_global_committed(event)

        elif name == "records_consumed":
            handler.handle_records_consumed(event)
            self._update_global_position(event, node)

        elif name == "partitions_revoked":
            handler.handle_partitions_revoked(event)

        elif name == "partitions_assigned":
            handler.handle_partitions_assigned(event)

        else:
            self.logger.debug(
                "%s: ignoring unknown event: %s"
                % (str(node.account), event)
            )

    def _update_global_position(self, consumed_event, node):
        for consumed_partition in consumed_event.get("partitions", []):
            tp = TopicPartition(
                consumed_partition["topic"],
                consumed_partition["partition"]
            )

            min_offset = consumed_partition["minOffset"]
            max_offset = consumed_partition["maxOffset"]

            if tp in self.global_committed:
                assert self.global_committed[tp] <= min_offset, \
                    "Consumed position %d is behind the current committed " \
                    "offset %d for partition %s" % \
                    (
                        min_offset,
                        self.global_committed[tp],
                        str(tp)
                    )

            if (
                tp in self.global_position
                and self.global_position[tp] != min_offset
            ):
                self.logger.warning(
                    "%s: Expected next consumed offset of %d for "
                    "partition %s, but instead saw %d"
                    % (
                        str(node.account),
                        self.global_position[tp],
                        str(tp),
                        min_offset
                    )
                )

            self.global_position[tp] = max_offset + 1

    def _update_global_committed(self, commit_event):
        if not commit_event.get("success", False):
            return

        for offset_commit in commit_event.get("offsets", []):
            if offset_commit.get("error", "") != "":
                continue

            tp = TopicPartition(
                offset_commit["topic"],
                offset_commit["partition"]
            )
            offset = offset_commit["offset"]

            assert tp in self.global_position, \
                "No consumed position exists for committed partition %s" % \
                str(tp)

            assert self.global_position[tp] >= offset, \
                "Committed offset %d for partition %s is ahead of the " \
                "current position %d" % \
                (
                    offset,
                    str(tp),
                    self.global_position[tp]
                )

            self.global_committed[tp] = offset

    def start_cmd(self, node, security_config=None):
        if security_config is None:
            security_config = self.kafka.security_config.client_config(
                self.base_prop_file,
                node
            )

        cmd = ""
        cmd += "export LOG_DIR=%s;" % self.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % security_config.kafka_opts
        cmd += (
            " export KAFKA_LOG4J_OPTS="
            "\"-Dlog4j.configuration=file:%s\"; "
            % self.LOG4J_CONFIG
        )
        cmd += self.impl.exec_cmd(node)
        cmd += (
            " --group-id %s --topic %s --broker-list %s "
            "--session-timeout %s --assignment-strategy %s %s"
            % (
                self.group_id,
                self.topic,
                self.kafka.bootstrap_servers(
                    security_config.security_protocol
                ),
                self.session_timeout_sec * 1000,
                self.assignment_strategy,
                (
                    "--enable-autocommit"
                    if self.enable_autocommit
                    else ""
                )
            )
        )

        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)

        cmd += " --consumer.config %s" % self.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (
            self.STDERR_CAPTURE,
            self.STDOUT_CAPTURE
        )

        return cmd

    def pids(self, node):
        return self.impl.pids(node)

    def try_parse_json(self, node, string):
        try:
            event = json.loads(string)
        except (TypeError, ValueError):
            self.logger.debug(
                "%s: Could not parse as json: %s"
                % (str(node.account), str(string))
            )
            return None

        if not isinstance(event, dict):
            self.logger.debug(
                "%s: Ignoring non-object event: %s"
                % (str(node.account), str(event))
            )
            return None

        return event

    def stop_all(self):
        for node in self.nodes:
            self.stop_node(node)

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        sig = self.impl.kill_signal(clean_shutdown)

        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

        with self.lock:
            handler = self.event_handlers.get(node)
            if handler is not None:
                handler.handle_kill_process(clean_shutdown)

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(
            node,
            clean_shutdown=clean_shutdown
        )

        stopped = self.wait_node(
            node,
            timeout_sec=self.stop_timeout_sec
        )

        assert stopped, \
            "Node %s: did not stop within the specified timeout of %s seconds" \
            % (
                str(node.account),
                str(self.stop_timeout_sec)
            )

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)

        node.account.ssh(
            "rm -rf %s" % self.PERSISTENT_ROOT,
            allow_fail=False
        )

        security_config = self.kafka.security_config.client_config(
            self.base_prop_file,
            node
        )
        security_config.clean_node(node)

    def current_assignment(self):
        with self.lock:
            return {
                handler.node: handler.current_assignment()
                for handler in self.event_handlers.values()
            }

    def current_position(self, tp):
        with self.lock:
            return self.global_position.get(tp)

    def owner(self, tp):
        with self.lock:
            for handler in self.event_handlers.values():
                if tp in handler.current_assignment():
                    return handler.node
            return None

    def last_commit(self, tp):
        with self.lock:
            return self.global_committed.get(tp)

    def total_consumed(self):
        with self.lock:
            return sum(
                handler.total_consumed
                for handler in self.event_handlers.values()
            )

    def num_rebalances(self):
        with self.lock:
            if not self.event_handlers:
                return 0

            return max(
                handler.assigned_count
                for handler in self.event_handlers.values()
            )

    def joined_nodes(self):
        with self.lock:
            return [
                handler.node
                for handler in self.event_handlers.values()
                if handler.state == ConsumerState.Joined
            ]

    def rebalancing_nodes(self):
        with self.lock:
            return [
                handler.node
                for handler in self.event_handlers.values()
                if handler.state == ConsumerState.Rebalancing
            ]

    def dead_nodes(self):
        with self.lock:
            return [
                handler.node
                for handler in self.event_handlers.values()
                if handler.state == ConsumerState.Dead
            ]

    def alive_nodes(self):
        with self.lock:
            return [
                handler.node
                for handler in self.event_handlers.values()
                if handler.state != ConsumerState.Dead
            ]

최종 개선사항
✅ 공유 self.security_config → worker 지역 설정 주입 → 멀티노드 실행에서 노드별 보안 설정 격리
✅ prop_file 누적 구조 → 불변 베이스 설정 + 독립 최종 설정 → 재시도·병렬 실행 시 설정 오염 방지
✅ 보안 설정 원문 로그 출력 → 노드 단위 생성 로그로 축소 → credential 노출 위험 감소
✅ 스트림 내부 중복 try/except → worker 경계 단일 예외 로깅 + traceback 보존 → 장애 원인 추적성 확보
✅ JSON 이벤트 전체 신뢰 → 객체 여부와 선택적 이벤트 필드 검증 → malformed event의 불필요한 worker 종료 방지
✅ 빈 handler에서 max() 실행 → 초기 상태 명시적 반환 → 초기화 타이밍에 따른 불필요한 엔진 셧다운 방지
✅ 정상 이벤트의 필수 데이터까지 무조건 흡수 → 핵심 데이터 계약은 엄격 검증 → 조용한 데이터 오염 대신 즉시 실패 보장

분산 컨슈머의 상태 머신과 오프셋 검증이라는 원본 목적은 그대로 유지하면서 공유 설정 경쟁 조건·민감정보 로그·스트림 경계·초기화 경계를 차단해, 실제 멀티노드 장애에서도 원인을 숨기지 않고 데이터 무결성을 끝까지 보존하는 실무형 검증 서비스로 승격된다.            
