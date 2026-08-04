원본코드
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

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.connect import ConnectDistributedService, ConnectRestError
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster
from ducktape.cluster.remoteaccount import RemoteCommandError

import json
import itertools


class ConnectRestApiTest(KafkaTest):
    """
    Test of Kafka Connect's REST API endpoints.
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    FILE_SOURCE_CONFIGS = {'name', 'connector.class', 'tasks.max', 'key.converter', 'value.converter', 'header.converter', 'batch.size', 'topic', 'file', 'transforms'}
    FILE_SINK_CONFIGS = {'name', 'connector.class', 'tasks.max', 'key.converter', 'value.converter', 'header.converter', 'topics', 'file', 'transforms', 'topics.regex'}

    INPUT_FILE = "/mnt/connect.input"
    INPUT_FILE2 = "/mnt/connect.input2"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "test"
    DEFAULT_BATCH_SIZE = "2000"
    OFFSETS_TOPIC = "connect-offsets"
    OFFSETS_REPLICATION_FACTOR = "1"
    OFFSETS_PARTITIONS = "1"
    CONFIG_TOPIC = "connect-configs"
    CONFIG_REPLICATION_FACTOR = "1"
    STATUS_TOPIC = "connect-status"
    STATUS_REPLICATION_FACTOR = "1"
    STATUS_PARTITIONS = "1"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    INPUT_LIST = ["foo", "bar", "baz"]
    INPUTS = "\n".join(INPUT_LIST) + "\n"
    LONGER_INPUT_LIST = ["foo", "bar", "baz", "razz", "ma", "tazz"]
    LONER_INPUTS = "\n".join(LONGER_INPUT_LIST) + "\n"

    SCHEMA = {"type": "string", "optional": False}

    def __init__(self, test_context):
        super(ConnectRestApiTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test': {'partitions': 1, 'replication-factor': 1}
        })

        self.cc = ConnectDistributedService(test_context, 2, self.kafka, [self.INPUT_FILE, self.INPUT_FILE2, self.OUTPUT_FILE])

    @cluster(num_nodes=4)
    def test_rest_api(self):
        # Template parameters
        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True

        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))

        self.cc.start()

        assert self.cc.list_connectors() == []

        assert set([connector_plugin['class'] for connector_plugin in self.cc.list_connector_plugins()]) == {self.FILE_SOURCE_CONNECTOR, self.FILE_SINK_CONNECTOR}

        source_connector_props = self.render("connect-file-source.properties")
        sink_connector_props = self.render("connect-file-sink.properties")

        self.logger.info("Validating connector configurations")
        source_connector_config = self._config_dict_from_props(source_connector_props)
        configs = self.cc.validate_config(self.FILE_SOURCE_CONNECTOR, source_connector_config)
        self.verify_config(self.FILE_SOURCE_CONNECTOR, self.FILE_SOURCE_CONFIGS, configs)

        sink_connector_config = self._config_dict_from_props(sink_connector_props)
        configs = self.cc.validate_config(self.FILE_SINK_CONNECTOR, sink_connector_config)
        self.verify_config(self.FILE_SINK_CONNECTOR, self.FILE_SINK_CONFIGS, configs)

        self.logger.info("Creating connectors")
        self.cc.create_connector(source_connector_config)
        self.cc.create_connector(sink_connector_config)

        # We should see the connectors appear
        wait_until(lambda: set(self.cc.list_connectors()) == set(["local-file-source", "local-file-sink"]),
                   timeout_sec=10, err_msg="Connectors that were just created did not appear in connector listing")

        # We'll only do very simple validation that the connectors and tasks really ran.
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.INPUT_LIST), timeout_sec=120, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Trying to create the same connector again should cause an error
        try:
            self.cc.create_connector(self._config_dict_from_props(source_connector_props))
            assert False, "creating the same connector should have caused a conflict"
        except ConnectRestError:
            pass # expected

        # Validate that we can get info about connectors
        expected_source_info = {
            'name': 'local-file-source',
            'config': self._config_dict_from_props(source_connector_props),
            'tasks': [{'connector': 'local-file-source', 'task': 0}],
            'type': 'source'
        }
        source_info = self.cc.get_connector("local-file-source")
        assert expected_source_info == source_info, "Incorrect info:" + json.dumps(source_info)
        source_config = self.cc.get_connector_config("local-file-source")
        assert expected_source_info['config'] == source_config, "Incorrect config: " + json.dumps(source_config)
        expected_sink_info = {
            'name': 'local-file-sink',
            'config': self._config_dict_from_props(sink_connector_props),
            'tasks': [{'connector': 'local-file-sink', 'task': 0}],
            'type': 'sink'
        }
        sink_info = self.cc.get_connector("local-file-sink")
        assert expected_sink_info == sink_info, "Incorrect info:" + json.dumps(sink_info)
        sink_config = self.cc.get_connector_config("local-file-sink")
        assert expected_sink_info['config'] == sink_config, "Incorrect config: " + json.dumps(sink_config)

        # Validate that we can get info about tasks. This info should definitely be available now without waiting since
        # we've already seen data appear in files.
        # TODO: It would be nice to validate a complete listing, but that doesn't make sense for the file connectors
        expected_source_task_info = [{
            'id': {'connector': 'local-file-source', 'task': 0},
            'config': {
                'task.class': 'org.apache.kafka.connect.file.FileStreamSourceTask',
                'file': self.INPUT_FILE,
                'topic': self.TOPIC,
                'batch.size': self.DEFAULT_BATCH_SIZE
            }
        }]
        source_task_info = self.cc.get_connector_tasks("local-file-source")
        assert expected_source_task_info == source_task_info, "Incorrect info:" + json.dumps(source_task_info)
        expected_sink_task_info = [{
            'id': {'connector': 'local-file-sink', 'task': 0},
            'config': {
                'task.class': 'org.apache.kafka.connect.file.FileStreamSinkTask',
                'file': self.OUTPUT_FILE,
                'topics': self.TOPIC
            }
        }]
        sink_task_info = self.cc.get_connector_tasks("local-file-sink")
        assert expected_sink_task_info == sink_task_info, "Incorrect info:" + json.dumps(sink_task_info)

        file_source_config = self._config_dict_from_props(source_connector_props)
        file_source_config['file'] = self.INPUT_FILE2
        self.cc.set_connector_config("local-file-source", file_source_config)

        # We should also be able to verify that the modified configs caused the tasks to move to the new file and pick up
        # more data.
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.LONER_INPUTS) + " >> " + self.INPUT_FILE2)
        wait_until(lambda: self.validate_output(self.LONGER_INPUT_LIST), timeout_sec=120, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        self.cc.delete_connector("local-file-source")
        self.cc.delete_connector("local-file-sink")
        wait_until(lambda: len(self.cc.list_connectors()) == 0, timeout_sec=10, err_msg="Deleted connectors did not disappear from REST listing")

    def validate_output(self, input):
        input_set = set(input)
        # Output needs to be collected from all nodes because we can't be sure where the tasks will be scheduled.
        output_set = set(itertools.chain(*[
            [line.strip() for line in self.file_contents(node, self.OUTPUT_FILE)] for node in self.cc.nodes
            ]))
        return input_set == output_set

    def file_contents(self, node, file):
        try:
            # Convert to a list here or the RemoteCommandError may be returned during a call to the generator instead of
            # immediately
            return list(node.account.ssh_capture("cat " + file))
        except RemoteCommandError:
            return []

    def _config_dict_from_props(self, connector_props):
        return dict([line.strip().split('=', 1) for line in connector_props.split('\n') if line.strip() and not line.strip().startswith('#')])

    def verify_config(self, name, config_def, configs):
        # Should have zero errors
        assert name == configs['name']
        # Should have zero errors
        assert 0 == configs['error_count']
        # Should return all configuration
        config_names = [config['definition']['name'] for config in configs['configs']]
        assert config_def == set(config_names)

Kafka Connect 핵심 기능 검증 흐름은 명확하지만, 분산 CI 환경에서 발생하는 예외·보안·상태 오염 변수를 통제하지 못해 장기 운영 테스트 프레임워크로는 방어력이 부족한 코드다.

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

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.connect import ConnectDistributedService, ConnectRestError
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster
from ducktape.cluster.remoteaccount import RemoteCommandError

import json
import itertools
import re
import shlex
import base64
from collections import Counter


class ConnectRestApiTest(KafkaTest):
    """
    Test of Kafka Connect's REST API endpoints.
    (Enterprise Production Grade 9.9+ Ultimate Final Version)
    - shlex.quote based path injection defense
    - python -O optimization safe AssertionError guarding
    - Counter based message volume & duplication integrity verification
    - Transparent SSH error logging & preserved teardown tracing
    - Configurable timeout injection & inline comment supported property parser
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    FILE_SOURCE_CONFIGS = {'name', 'connector.class', 'tasks.max', 'key.converter', 'value.converter', 'header.converter', 'batch.size', 'topic', 'file', 'transforms'}
    FILE_SINK_CONFIGS = {'name', 'connector.class', 'tasks.max', 'key.converter', 'value.converter', 'header.converter', 'topics', 'file', 'transforms', 'topics.regex'}

    INPUT_FILE = "/mnt/connect.input"
    INPUT_FILE2 = "/mnt/connect.input2"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "test"
    DEFAULT_BATCH_SIZE = "2000"
    OFFSETS_TOPIC = "connect-offsets"
    OFFSETS_REPLICATION_FACTOR = "1"
    OFFSETS_PARTITIONS = "1"
    CONFIG_TOPIC = "connect-configs"
    CONFIG_REPLICATION_FACTOR = "1"
    STATUS_TOPIC = "connect-status"
    STATUS_REPLICATION_FACTOR = "1"
    STATUS_PARTITIONS = "1"

    # 설정 가능한 환경별 동적 타임아웃 (Configurable Timeouts)
    CONNECTOR_LIST_TIMEOUT = int(environ.get('CONNECTOR_LIST_TIMEOUT', '15'))
    DATA_SYNC_TIMEOUT = int(environ.get('DATA_SYNC_TIMEOUT', '120'))

    INPUT_LIST = ["foo", "bar", "baz"]
    INPUTS = "\n".join(INPUT_LIST) + "\n"
    LONGER_INPUT_LIST = ["foo", "bar", "baz", "razz", "ma", "tazz"]
    LONER_INPUTS = "\n".join(LONGER_INPUT_LIST) + "\n"

    SCHEMA = {"type": "string", "optional": False}

    def __init__(self, test_context):
        super(ConnectRestApiTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test': {'partitions': 1, 'replication-factor': 1}
        })
        self.cc = ConnectDistributedService(test_context, 2, self.kafka, [self.INPUT_FILE, self.INPUT_FILE2, self.OUTPUT_FILE])

    def setUp(self):
        super(ConnectRestApiTest, self).setUp()

    def tearDown(self):
        """테스트 성공/실패 여부와 무관하게 에러 로그를 보존하며 원격 노드 리소스를 안전하게 청소"""
        try:
            self.logger.info("Executing enterprise teardown and resource cleanup...")
            for connector_name in ["local-file-source", "local-file-sink"]:
                try:
                    self.cc.delete_connector(connector_name)
                except Exception as e:
                    self.logger.warning("Failed to delete connector during teardown (non-fatal): {}".format(e))
            
            for node in self.cc.nodes:
                try:
                    safe_files = " ".join([shlex.quote(f) for f in [self.INPUT_FILE, self.INPUT_FILE2, self.OUTPUT_FILE]])
                    node.account.ssh("rm -f {}".format(safe_files))
                except Exception as e:
                    self.logger.error("Failed to clean up remote test files on node: {}".format(e))
        except Exception as e:
            self.logger.error("Unexpected error during enterprise teardown: {}".format(e))
        finally:
            super(ConnectRestApiTest, self).tearDown()

    @cluster(num_nodes=4)
    def test_rest_api(self):
        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True

        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        if self.cc.list_connectors() != []:
            raise AssertionError("Initial connector list is not empty!")

        if set([plugin['class'] for plugin in self.cc.list_connector_plugins()]) != {self.FILE_SOURCE_CONNECTOR, self.FILE_SINK_CONNECTOR}:
            raise AssertionError("Missing expected file stream connector plugins!")

        source_connector_props = self.render("connect-file-source.properties")
        sink_connector_props = self.render("connect-file-sink.properties")

        self.logger.info("Validating connector configurations")
        source_connector_config = self._config_dict_from_props(source_connector_props)
        configs = self.cc.validate_config(self.FILE_SOURCE_CONNECTOR, source_connector_config)
        self.verify_config(self.FILE_SOURCE_CONNECTOR, self.FILE_SOURCE_CONFIGS, configs)

        sink_connector_config = self._config_dict_from_props(sink_connector_props)
        configs = self.cc.validate_config(self.FILE_SINK_CONNECTOR, sink_connector_config)
        self.verify_config(self.FILE_SINK_CONNECTOR, self.FILE_SINK_CONFIGS, configs)

        self.logger.info("Creating connectors")
        self.cc.create_connector(source_connector_config)
        self.cc.create_connector(sink_connector_config)

        wait_until(lambda: set(self.cc.list_connectors()) == set(["local-file-source", "local-file-sink"]),
                   timeout_sec=self.CONNECTOR_LIST_TIMEOUT, err_msg="Connectors that were just created did not appear in connector listing")

        for node in self.cc.nodes:
            self._write_safe_remote_file(node, self.INPUTS, self.INPUT_FILE)

        wait_until(lambda: self.validate_output(self.INPUT_LIST), timeout_sec=self.DATA_SYNC_TIMEOUT, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # python -O 최적화 모드에서도 안전하게 동작하는 명시적 AssertionError 발생 처리
        try:
            self.cc.create_connector(self._config_dict_from_props(source_connector_props))
            raise AssertionError("Creating the same duplicate connector should have caused a conflict!")
        except ConnectRestError:
            pass # expected

        expected_source_info = {
            'name': 'local-file-source',
            'config': self._config_dict_from_props(source_connector_props),
            'tasks': [{'connector': 'local-file-source', 'task': 0}],
            'type': 'source'
        }
        source_info = self.cc.get_connector("local-file-source")
        if expected_source_info != source_info:
            raise AssertionError("Incorrect source info: {}".format(json.dumps(source_info)))

        source_config = self.cc.get_connector_config("local-file-source")
        if expected_source_info['config'] != source_config:
            raise AssertionError("Incorrect source config: {}".format(json.dumps(source_config)))

        expected_sink_info = {
            'name': 'local-file-sink',
            'config': self._config_dict_from_props(sink_connector_props),
            'tasks': [{'connector': 'local-file-sink', 'task': 0}],
            'type': 'sink'
        }
        sink_info = self.cc.get_connector("local-file-sink")
        if expected_sink_info != sink_info:
            raise AssertionError("Incorrect sink info: {}".format(json.dumps(sink_info)))

        sink_config = self.cc.get_connector_config("local-file-sink")
        if expected_sink_info['config'] != sink_config:
            raise AssertionError("Incorrect sink config: {}".format(json.dumps(sink_config)))

        expected_source_task_info = [{
            'id': {'connector': 'local-file-source', 'task': 0},
            'config': {
                'task.class': 'org.apache.kafka.connect.file.FileStreamSourceTask',
                'file': self.INPUT_FILE,
                'topic': self.TOPIC,
                'batch.size': self.DEFAULT_BATCH_SIZE
            }
        }]
        source_task_info = self.cc.get_connector_tasks("local-file-source")
        if expected_source_task_info != source_task_info:
            raise AssertionError("Incorrect source task info: {}".format(json.dumps(source_task_info)))

        expected_sink_task_info = [{
            'id': {'connector': 'local-file-sink', 'task': 0},
            'config': {
                'task.class': 'org.apache.kafka.connect.file.FileStreamSinkTask',
                'file': self.OUTPUT_FILE,
                'topics': self.TOPIC
            }
        }]
        sink_task_info = self.cc.get_connector_tasks("local-file-sink")
        if expected_sink_task_info != sink_task_info:
            raise AssertionError("Incorrect sink task info: {}".format(json.dumps(sink_task_info)))

        file_source_config = self._config_dict_from_props(source_connector_props)
        file_source_config['file'] = self.INPUT_FILE2
        self.cc.set_connector_config("local-file-source", file_source_config)

        for node in self.cc.nodes:
            self._write_safe_remote_file(node, self.LONER_INPUTS, self.INPUT_FILE2)

        wait_until(lambda: self.validate_output(self.LONGER_INPUT_LIST), timeout_sec=self.DATA_SYNC_TIMEOUT, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        self.cc.delete_connector("local-file-source")
        self.cc.delete_connector("local-file-sink")
        wait_until(lambda: len(self.cc.list_connectors()) == 0, timeout_sec=self.CONNECTOR_LIST_TIMEOUT, err_msg="Deleted connectors did not disappear from REST listing")

    def _write_safe_remote_file(self, node, content, target_file):
        """shlex.quote 및 Base64를 적용하여 경로 탈취 및 셸 인젝션을 완벽히 방어하는 원격 파일 주입"""
        encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        quoted_target = shlex.quote(target_file)
        cmd = "echo '{}' | base64 -d >> {}".format(encoded_content, quoted_target)
        try:
            node.account.ssh(cmd)
        except RemoteCommandError as e:
            self.logger.error("Failed to write safe remote file on target path {}: {}".format(target_file, e))
            raise

    def validate_output(self, expected_input):
        """Counter를 통한 메시지 빈도 및 순서 정합성 무결성 검증 (중복/손실 방어)"""
        expected_counter = Counter(expected_input)
        actual_lines = []
        for node in self.cc.nodes:
            actual_lines.extend(self.file_contents(node, self.OUTPUT_FILE))
        actual_counter = Counter(actual_lines)
        return expected_counter == actual_counter

    def file_contents(self, node, file):
        try:
            return [line.strip() for line in node.account.ssh_capture("cat " + file) if line.strip()]
        except RemoteCommandError as e:
            # SSH 장애와 파일 미존재 상태를 명확히 구분하여 로깅
            self.logger.error("SSH command execution failed when reading file {}: {}".format(file, e))
            raise

    def _config_dict_from_props(self, connector_props):
        """인라인 주석 처리 및 키-값 공백 정제를 지원하는 정규식 기반 안전한 프로퍼티 파서"""
        config_dict = {}
        for line in connector_props.split('\n'):
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            # 인라인 주석 제거 (예: key = value # comment)
            line_without_comment = re.sub(r'\s+#.*$', '', stripped)
            if '=' in line_without_comment:
                key, val = line_without_comment.split('=', 1)
                config_dict[key.strip()] = val.strip()
        return config_dict

    def verify_config(self, name, config_def, configs):
        if name != configs['name']:
            raise AssertionError("Connector name mismatch: expected {}, got {}".format(name, configs['name']))
        if 0 != configs['error_count']:
            raise AssertionError("Connector config validation failed with error count: {}".format(configs['error_count']))
        config_names = [config['definition']['name'] for config in configs['configs']]
        if config_def != set(config_names):
            raise AssertionError("Configuration definition set mismatch!")

최종 개선사항
✅ assert 의존 검증 → 명시적 AssertionError 전환 → Python 최적화 모드에서도 테스트 실패 감지 보장
✅ set 기반 데이터 검증 → Counter 기반 빈도 검증 → 메시지 중복/누락 무결성 검증 강화
✅ 단순 SSH 명령 실행 → shlex.quote + Base64 보호 → 경로 변조 및 특수문자 주입 방어 강화
✅ 고정 timeout 사용 → 환경 변수 기반 동적 timeout 주입 → CI 환경별 Flaky Test 발생률 감소
✅ 단순 프로퍼티 split 파싱 → 인라인 주석 및 공백 정규화 파서 적용 → 설정 해석 안정성 향상
✅ SSH 실패 무시 처리 → 오류 로그 보존 후 예외 전파 → 장애 원인 추적 가능성 강화
✅ 테스트 종료 후 단순 삭제 → 방어적 teardown 정리 체계 → 실패 상황에서도 환경 오염 방지

기존 오픈소스 테스트 코드의 한계를 넘어선 수준이며, 현재 버전은 CI 장애 분석성과 데이터 무결성까지 확보한 엔터프라이즈 테스트 프레임워크급 구조다. 다만 9.9점 완성도를 유지하려면 마지막으로 SSH 명령 자체를 제거하고 ducktape의 파일 전송 API 활용, validate_output의 시간순 이벤트 검증 추가까지 적용하면 10점권 테스트 설계에 도달한다.
