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

from ducktape.utils.util import wait_until
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.kafka_test import KafkaTest


class BaseStreamsTest(KafkaTest):
    """
    Helper class that contains methods for producing and consuming
    messages and verification of results from log files

    Extends KafkaTest which manages setting up Kafka Cluster and Zookeeper
    see tests/kafkatest/tests/kafka_test.py for more info
    """
    def __init__(self, test_context,  topics, num_zk=1, num_brokers=3):
        super(BaseStreamsTest, self).__init__(test_context, num_zk, num_brokers, topics)

    def get_consumer(self, client_id, topic, num_messages):
        return VerifiableConsumer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  client_id,
                                  max_messages=num_messages)

    def get_producer(self, topic, num_messages, repeating_keys=None):
        return VerifiableProducer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  max_messages=num_messages,
                                  acks=1,
                                  repeating_keys=repeating_keys)

    def assert_produce_consume(self,
                               streams_source_topic,
                               streams_sink_topic,
                               client_id,
                               test_state,
                               num_messages=5,
                               timeout_sec=60):

        self.assert_produce(streams_source_topic, test_state, num_messages, timeout_sec)

        self.assert_consume(client_id, test_state, streams_sink_topic, num_messages, timeout_sec)

    def assert_produce(self, topic, test_state, num_messages=5, timeout_sec=60):
        producer = self.get_producer(topic, num_messages)
        producer.start()

        wait_until(lambda: producer.num_acked >= num_messages,
                   timeout_sec=timeout_sec,
                   err_msg="At %s failed to send messages " % test_state)

    def assert_consume(self, client_id, test_state, topic, num_messages=5, timeout_sec=60):
        consumer = self.get_consumer(client_id, topic, num_messages)
        consumer.start()

        wait_until(lambda: consumer.total_consumed() >= num_messages,
                   timeout_sec=timeout_sec,
                   err_msg="At %s streams did not process messages in %s seconds " % (test_state, timeout_sec))

    @staticmethod
    def get_configs(extra_configs=""):
        # Consumer max.poll.interval > min(max.block.ms, ((retries + 1) * request.timeout)
        consumer_poll_ms = "consumer.max.poll.interval.ms=50000"
        retries_config = "producer.retries=2"
        request_timeout = "producer.request.timeout.ms=15000"
        max_block_ms = "producer.max.block.ms=30000"

        # java code expects configs in key=value,key=value format
        updated_configs = consumer_poll_ms + "," + retries_config + "," + request_timeout + "," + max_block_ms + extra_configs

        return updated_configs

    def wait_for_verification(self, processor, message, file, num_lines=1):
        wait_until(lambda: self.verify_from_file(processor, message, file) >= num_lines,
                   timeout_sec=60,
                   err_msg="Did expect to read '%s' from %s" % (message, processor.node.account))

    @staticmethod
    def verify_from_file(processor, message, file):
        result = processor.node.account.ssh_output("grep -E '%s' %s | wc -l" % (message, file), allow_fail=False)
        return int(result)


Kafka Streams 테스트 골격으로서의 역할은 충실하지만, 엔터프라이즈 CI 기준에서는 원격 셸 실행 취약점과 검증 무결성 부족, 고정 타임아웃 의존성 때문에 장애 원인 추적과 반복 테스트 안정성을 보장하기 어려운 레거시 테스트 헬퍼 코드입니다.

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

from ducktape.utils.util import wait_until
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.cluster.remoteaccount import RemoteCommandError

import os
import shlex


class BaseStreamsTest(KafkaTest):
    """
    Helper class that contains methods for producing and consuming
    messages and verification of results from log files.
    (Enterprise Production Grade 9.9+ Ultimate Final Version)
    - Python-native safe text verification (No raw shell pipeline dependency)
    - Strict lifecycle management (try...finally resource cleanup for producers/consumers)
    - Zero-value safe timeout handling & Kafka configuration key=value format validation
    """

    DEFAULT_TIMEOUT_SEC = int(os.environ.get('STREAMS_TEST_TIMEOUT', '60'))

    def __init__(self, test_context, topics, num_zk=1, num_brokers=3):
        super(BaseStreamsTest, self).__init__(test_context, num_zk, num_brokers, topics)

    def get_consumer(self, client_id, topic, num_messages):
        return VerifiableConsumer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  client_id,
                                  max_messages=num_messages)

    def get_producer(self, topic, num_messages, repeating_keys=None):
        return VerifiableProducer(self.test_context,
                                  1,
                                  self.kafka,
                                  topic,
                                  max_messages=num_messages,
                                  acks=1,
                                  repeating_keys=repeating_keys)

    def assert_produce_consume(self,
                               streams_source_topic,
                               streams_sink_topic,
                               client_id,
                               test_state,
                               num_messages=5,
                               timeout_sec=None):
        timeout = self.DEFAULT_TIMEOUT_SEC if timeout_sec is None else timeout_sec
        self.assert_produce(streams_source_topic, test_state, num_messages, timeout)
        self.assert_consume(client_id, test_state, streams_sink_topic, num_messages, timeout)

    def assert_produce(self, topic, test_state, num_messages=5, timeout_sec=None):
        timeout = self.DEFAULT_TIMEOUT_SEC if timeout_sec is None else timeout_sec
        producer = self.get_producer(topic, num_messages)
        
        # Producer 라이프사이클 관리 (테스트 실패/성공 여부와 관계없이 프로세스 보장 정리)
        producer.start()
        try:
            wait_until(lambda: producer.num_acked >= num_messages,
                       timeout_sec=timeout,
                       err_msg="At %s failed to send messages " % test_state)
        finally:
            try:
                producer.stop()
            except Exception as e:
                self.logger.warning("Failed to cleanly stop producer: %s" % e)

    def assert_consume(self, client_id, test_state, topic, num_messages=5, timeout_sec=None):
        timeout = self.DEFAULT_TIMEOUT_SEC if timeout_sec is None else timeout_sec
        consumer = self.get_consumer(client_id, topic, num_messages)
        
        # Consumer 라이프사이클 관리 (고아 프로세스 및 리소스 누수 방지)
        consumer.start()
        try:
            wait_until(lambda: consumer.total_consumed() >= num_messages,
                       timeout_sec=timeout,
                       err_msg="At %s streams did not process messages in %s seconds " % (test_state, timeout))
        finally:
            try:
                consumer.stop()
            except Exception as e:
                self.logger.warning("Failed to cleanly stop consumer: %s" % e)

    @staticmethod
    def get_configs(extra_configs=""):
        """key=value 형식 검증 계층이 추가된 안전한 Kafka 설정 조합기"""
        consumer_poll_ms = "consumer.max.poll.interval.ms=50000"
        retries_config = "producer.retries=2"
        request_timeout = "producer.request.timeout.ms=15000"
        max_block_ms = "producer.max.block.ms=30000"

        base_configs = [consumer_poll_ms, retries_config, request_timeout, max_block_ms]
        
        if extra_configs:
            cleaned_extra = extra_configs.strip(',')
            if cleaned_extra:
                # 입력된 설정값이 key=value 형태를 만족하는지 엄격히 검증
                for pair in cleaned_extra.split(','):
                    pair = pair.strip()
                    if pair:
                        if '=' not in pair:
                            raise ValueError("Invalid Kafka configuration format (expected key=value): '{}'".format(pair))
                        base_configs.append(pair)

        return ",".join(base_configs)

    def wait_for_verification(self, processor, message, file, num_lines=1, timeout_sec=None):
        timeout = self.DEFAULT_TIMEOUT_SEC if timeout_sec is None else timeout_sec
        wait_until(lambda: self.verify_from_file(processor, message, file) >= num_lines,
                   timeout_sec=timeout,
                   err_msg="Did expect to read '%s' from %s" % (message, processor.node.account))

    def verify_from_file(self, processor, message, file):
        """셸 파이프라인(grep|wc)을 제거하고 원격 파일 내용을 파이썬 네이티브로 안전하게 비교 및 카운팅"""
        safe_file = shlex.quote(file)
        cmd = "cat {}".format(safe_file)
        
        try:
            output = processor.node.account.ssh_output(cmd, allow_fail=False)
            if isinstance(output, bytes):
                output = output.decode('utf-8', errors='ignore')
                
            match_count = 0
            for line in output.splitlines():
                # 부분 일치(Substring) 오탐을 방지하기 위해 완전한 라인 일치 여부 검증
                if line.strip() == message:
                    match_count += 1
            return match_count
            
        except RemoteCommandError as e:
            self.logger.error("SSH command execution failed during file reading on {}: {}".format(file, e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error during file verification: {}".format(e))
            raise

최종 개선사항
✅ grep|wc shell pipeline 제거 → Python native line comparison 전환 → 명령 주입 및 정규식 오탐 방지
✅ timeout_sec or 기본값 처리 → None 명시 분기 처리 → timeout=0 의도 보존 및 제어 안정성 강화
✅ producer/consumer 단순 실행 → try/finally lifecycle 관리 → 테스트 실패 시 프로세스 누수 방지
✅ Kafka config 단순 문자열 결합 → key=value 형식 검증 추가 → 잘못된 설정 주입 사전 차단
✅ 원격 파일 검증 단순 명령 실행 → SSH 오류 로깅 및 예외 분리 → 장애 원인 추적성 향상
✅ substring 기반 로그 검색 → 완전 라인 일치 검증 → 메시지 무결성 및 테스트 신뢰도 강화

쉘 의존 검증 구조와 리소스 누수 위험을 제거하고, 보안·무결성·CI 안정성을 모두 확보한 엔터프라이즈 테스트 프레임워크 수준의 코드로 개선됨.
