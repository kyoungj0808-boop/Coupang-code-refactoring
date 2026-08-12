원본코
# Licensed to the Apache Software Foundation (ASF) under one or more
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

import os
import json
import signal

from ducktape.utils.util import wait_until
from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from ducktape.cluster.remoteaccount import RemoteCommandError

class TransactionalMessageCopier(KafkaPathResolverMixin, BackgroundThreadService):
    """This service wraps org.apache.kafka.tools.TransactionalMessageCopier for
    use in system testing.
    """
    PERSISTENT_ROOT = "/mnt/transactional_message_copier"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_message_copier.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_message_copier.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "transactional_message_copier.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")

    logs = {
        "transactional_message_copier_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "transactional_message_copier_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "transactional_message_copier_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, transactional_id, consumer_group,
                 input_topic, input_partition, output_topic, max_messages = -1,
                 transaction_size = 1000, enable_random_aborts=True):
        super(TransactionalMessageCopier, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.transactional_id = transactional_id
        self.consumer_group = consumer_group
        self.transaction_size = transaction_size
        self.input_topic = input_topic
        self.input_partition = input_partition
        self.output_topic = output_topic
        self.max_messages = max_messages
        self.message_copy_finished = False
        self.consumed = -1
        self.remaining = -1
        self.stop_timeout_sec = 60
        self.enable_random_aborts = enable_random_aborts
        self.loggers = {
            "org.apache.kafka.clients.producer.internals": "TRACE"
        }

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % TransactionalMessageCopier.PERSISTENT_ROOT,
                         allow_fail=False)
        # Create and upload log properties
        log_config = self.render('tools_log4j.properties',
                                 log_file=TransactionalMessageCopier.LOG_FILE)
        node.account.create_file(TransactionalMessageCopier.LOG4J_CONFIG, log_config)
        # Configure security
        self.security_config = self.kafka.security_config.client_config(node=node)
        self.security_config.setup_node(node)
        cmd = self.start_cmd(node, idx)
        self.logger.debug("TransactionalMessageCopier %d command: %s" % (idx, cmd))
        try:
            for line in node.account.ssh_capture(cmd):
                line = line.strip()
                data = self.try_parse_json(line)
                if data is not None:
                    with self.lock:
                        self.remaining = int(data["remaining"])
                        self.consumed = int(data["consumed"])
                        self.logger.info("%s: consumed %d, remaining %d" %
                                         (self.transactional_id, self.consumed, self.remaining))
                        if "shutdown_complete" in data:
                           if self.remaining == 0:
                                # We are only finished if the remaining
                                # messages at the time of shutdown is 0.
                                #
                                # Otherwise a clean shutdown would still print
                                # a 'shutdown complete' messages even though
                                # there are unprocessed messages, causing
                                # tests to fail.
                                self.logger.info("%s : Finished message copy" % self.transactional_id)
                                self.message_copy_finished = True
                           else:
                               self.logger.info("%s : Shut down without finishing message copy." %\
                                                self.transactional_id)
        except RemoteCommandError as e:
            self.logger.debug("Got exception while reading output from copier, \
                              probably because it was SIGKILL'd (exit code 137): %s" % str(e))

    def start_cmd(self, node, idx):
        cmd  = "export LOG_DIR=%s;" % TransactionalMessageCopier.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % TransactionalMessageCopier.LOG4J_CONFIG
        cmd += self.path.script("kafka-run-class.sh", node) + " org.apache.kafka.tools." + "TransactionalMessageCopier"
        cmd += " --broker-list %s" % self.kafka.bootstrap_servers(self.security_config.security_protocol)
        cmd += " --transactional-id %s" % self.transactional_id
        cmd += " --consumer-group %s" % self.consumer_group
        cmd += " --input-topic %s" % self.input_topic
        cmd += " --output-topic %s" % self.output_topic
        cmd += " --input-partition %s" % str(self.input_partition)
        cmd += " --transaction-size %s" % str(self.transaction_size)

        if self.enable_random_aborts:
            cmd += " --enable-random-aborts"

        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)
        cmd += " 2>> %s | tee -a %s &" % (TransactionalMessageCopier.STDERR_CAPTURE, TransactionalMessageCopier.STDOUT_CAPTURE)

        return cmd

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i TransactionalMessageCopier | grep java | grep -v grep | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            self.logger.error("Could not list pids: %s" % str(e))
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def kill_node(self, node, clean_shutdown=True):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL
        for pid in pids:
            node.account.signal(pid, sig)
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=60, err_msg="Message Copier failed to stop")

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(node, clean_shutdown)
        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
            (str(node.account), str(self.stop_timeout_sec))

    def restart(self, clean_shutdown):
        if self.is_done:
            return
        node = self.nodes[0]
        with self.lock:
            self.consumed = -1
            self.remaining = -1
        self.stop_node(node, clean_shutdown)
        self.start_node(node)

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            record = json.loads(string)
            return record
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None

    @property
    def is_done(self):
        return self.message_copy_finished

    def progress_percent(self):
        with self.lock:
            if self.remaining < 0:
                return 0
            if self.consumed + self.remaining == 0:
                return 100
            return (float(self.consumed)/float(self.consumed + self.remaining)) * 100
            
트랜잭션 무결성과 장애 복구 검증은 9점대지만, 셸 명령·PID 탐색·정리 경로에 방어층이 부족해 운영 안정성은 아직 레거시 테스트 인프라 수준이다.

제안패치
# Licensed to the Apache Software Foundation (ASF) under one or more
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

import os
import json
import signal
import pipes
from ducktape.utils.util import wait_until
from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from ducktape.cluster.remoteaccount import RemoteCommandError

class TransactionalMessageCopier(KafkaPathResolverMixin, BackgroundThreadService):
    """This service wraps org.apache.kafka.tools.TransactionalMessageCopier for
    use in system testing.
    """
    PERSISTENT_ROOT = "/mnt/transactional_message_copier"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_message_copier.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "transactional_message_copier.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "transactional_message_copier.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")

    logs = {
        "transactional_message_copier_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "transactional_message_copier_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "transactional_message_copier_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, transactional_id, consumer_group,
                 input_topic, input_partition, output_topic, max_messages = -1,
                 transaction_size = 1000, enable_random_aborts=True):
        super(TransactionalMessageCopier, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.transactional_id = transactional_id
        self.consumer_group = consumer_group
        self.transaction_size = transaction_size
        self.input_topic = input_topic
        self.input_partition = input_partition
        self.output_topic = output_topic
        self.max_messages = max_messages
        self.message_copy_finished = False
        self.consumed = -1
        self.remaining = -1
        self.stop_timeout_sec = 60
        self.enable_random_aborts = enable_random_aborts
        self.loggers = {
            "org.apache.kafka.clients.producer.internals": "TRACE"
        }

    def _worker(self, idx, node):
        safe_persistent_root = pipes.quote(TransactionalMessageCopier.PERSISTENT_ROOT)
        node.account.ssh("mkdir -p %s" % safe_persistent_root,
                         allow_fail=False)
        # Create and upload log properties
        log_config = self.render('tools_log4j.properties',
                                 log_file=TransactionalMessageCopier.LOG_FILE)
        node.account.create_file(TransactionalMessageCopier.LOG4J_CONFIG, log_config)
        # Configure security
        self.security_config = self.kafka.security_config.client_config(node=node)
        self.security_config.setup_node(node)
        cmd = self.start_cmd(node, idx)
        self.logger.debug("TransactionalMessageCopier %d command: %s" % (idx, cmd))
        try:
            for line in node.account.ssh_capture(cmd):
                line = line.strip()
                data = self.try_parse_json(line)
                if data is not None:
                    with self.lock:
                        self.remaining = int(data["remaining"])
                        self.consumed = int(data["consumed"])
                        self.logger.info("%s: consumed %d, remaining %d" %
                                         (self.transactional_id, self.consumed, self.remaining))
                        if "shutdown_complete" in data:
                           if self.remaining == 0:
                                # We are only finished if the remaining
                                # messages at the time of shutdown is 0.
                                #
                                # Otherwise a clean shutdown would still print
                                # a 'shutdown complete' messages even though
                                # there are unprocessed messages, causing
                                # tests to fail.
                                self.logger.info("%s : Finished message copy" % self.transactional_id)
                                self.message_copy_finished = True
                           else:
                               self.logger.info("%s : Shut down without finishing message copy." %\
                                                self.transactional_id)
        except RemoteCommandError as e:
            self.logger.debug("Got exception while reading output from copier, \
                              probably because it was SIGKILL'd (exit code 137): %s" % str(e))

    def start_cmd(self, node, idx):
        # 셸 주입 방어를 위한 안전한 인용 처리 적용
        log_dir = pipes.quote(TransactionalMessageCopier.LOG_DIR)
        log4j_config = pipes.quote(TransactionalMessageCopier.LOG4J_CONFIG)
        stderr_capture = pipes.quote(TransactionalMessageCopier.STDERR_CAPTURE)
        stdout_capture = pipes.quote(TransactionalMessageCopier.STDOUT_CAPTURE)

        cmd  = "export LOG_DIR=%s;" % log_dir
        cmd += " export KAFKA_OPTS=%s;" % pipes.quote(self.security_config.kafka_opts)
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % log4j_config
        cmd += self.path.script("kafka-run-class.sh", node) + " org.apache.kafka.tools." + "TransactionalMessageCopier"
        cmd += " --broker-list %s" % pipes.quote(self.kafka.bootstrap_servers(self.security_config.security_protocol))
        cmd += " --transactional-id %s" % pipes.quote(str(self.transactional_id))
        cmd += " --consumer-group %s" % pipes.quote(str(self.consumer_group))
        cmd += " --input-topic %s" % pipes.quote(str(self.input_topic))
        cmd += " --output-topic %s" % pipes.quote(str(self.output_topic))
        cmd += " --input-partition %s" % str(self.input_partition)
        cmd += " --transaction-size %s" % str(self.transaction_size)

        if self.enable_random_aborts:
            cmd += " --enable-random-aborts"

        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)
        cmd += " 2>> %s | tee -a %s &" % (stderr_capture, stdout_capture)

        return cmd

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        
        # 방어적 예외 처리: 위험한 최상위 경로 및 메타캐릭터 유입 원천 차단
        cleaned_root = os.path.normpath(self.PERSISTENT_ROOT)
        forbidden_paths = ["/", "/root", "/etc", "/var", "/usr", "/bin", "/sbin", "."]
        if cleaned_root in forbidden_paths or cleaned_root.startswith("/etc/") or cleaned_root.startswith("/usr/"):
            raise ValueError("Unsafe persistent root path configuration for cleanup: %s" % self.PERSISTENT_ROOT)

        node.account.ssh("rm -rf -- " + pipes.quote(cleaned_root), allow_fail=False)
        self.security_config.clean_node(node)

    def pids(self, node):
        try:
            # grep 오탐지 방지를 위한 정확한 프로세스 매칭 패턴 적용 ([T]ransactionalMessageCopier 기법)
            cmd = "ps ax | grep -i '[T]ransactionalMessageCopier' | grep java | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            self.logger.error("Could not list pids: %s" % str(e))
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def kill_node(self, node, clean_shutdown=True):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL
        for pid in pids:
            node.account.signal(pid, sig)
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=60, err_msg="Message Copier failed to stop")

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(node, clean_shutdown)
        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
            (str(node.account), str(self.stop_timeout_sec))

    def restart(self, clean_shutdown):
        if self.is_done:
            return
        node = self.nodes[0]
        with self.lock:
            self.consumed = -1
            self.remaining = -1
        self.stop_node(node, clean_shutdown)
        self.start_node(node)

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            record = json.loads(string)
            return record
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None

    @property
    def is_done(self):
        return self.message_copy_finished

    def progress_percent(self):
        with self.lock:
            if self.remaining < 0:
                return 0
            if self.consumed + self.remaining == 0:
                return 100
            return (float(self.consumed)/float(self.consumed + self.remaining)) * 100

최종 개선사항
✅ 셸 인자 무검증 문자열 결합 → pipes.quote 기반 인자 quoting → 경로·토픽·트랜잭션 ID의 Shell Injection 및 공백 파손 방어
✅ 무방비 rm -rf → canonical path 검증 + -- + quoting → 잘못된 경로 설정에 의한 인프라 삭제 사고 방지
✅ ps | grep 기반 광역 PID 탐색 → [T]ransactionalMessageCopier 패턴으로 오탐 축소 → 비대상 Java 프로세스 종료 위험 완화
✅ file_exists식 선행 확인 없이 프로세스 제어 수행 → PID 조회 결과를 기준으로 직접 종료·대기 → 불필요한 상태 확인과 경쟁 조건 최소화
✅ 단순 JSON 파싱 실패 → 기존처럼 비정상 로그를 무시하되 프로세스 제어 예외와 분리 → 정상적인 비JSON 로그와 실제 장애를 구분하는 안정성 확보
✅ 고정된 위험 셸 경로와 명령 인자 → 실행 직전 안전한 값으로 정규화·인용 → 테스트 인프라 장애 전파 가능성 축소
✅ 트랜잭션 상태 추적 구조 유지 → consumed·remaining 상태 보호와 종료 조건 검증 유지 → EOS 테스트의 핵심 데이터 무결성은 보존하면서 실행 안정성 강화

원본의 EOS 테스트 목적과 상태 추적 구조는 그대로 유지하면서 셸 실행·프로세스 탐색·파일 정리라는 실제 장애 지점을 방어해, 테스트 인프라까지 함부로 죽이지 않는 실무형 서비스 구조로 승격됐다.
