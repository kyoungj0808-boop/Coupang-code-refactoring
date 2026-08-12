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

import time
from kafkatest.services.streams import StreamsBrokerDownResilienceService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest


class StreamsBrokerDownResilience(BaseStreamsTest):
    """
    This test validates that Streams is resilient to a broker
    being down longer than specified timeouts in configs
    """

    inputTopic = "streamsResilienceSource"
    outputTopic = "streamsResilienceSink"
    client_id = "streams-broker-resilience-verify-consumer"
    num_messages = 5

    def __init__(self, test_context):
        super(StreamsBrokerDownResilience, self).__init__(test_context,
                                                          topics={self.inputTopic: {'partitions': 3, 'replication-factor': 1},
                                                                  self.outputTopic: {'partitions': 1, 'replication-factor': 1}},
                                                          num_brokers=1)

    def setUp(self):
        self.zk.start()

    def test_streams_resilient_to_broker_down(self):
        self.kafka.start()

        # Broker should be down over 2x of retries * timeout ms
        # So with (2 * 15000) = 30 seconds, we'll set downtime to 70 seconds
        broker_down_time_in_seconds = 70

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, self.get_configs())
        processor.start()

        # until KIP-91 is merged we'll only send 5 messages to assert Kafka Streams is running before taking the broker down
        # After KIP-91 is merged we'll continue to send messages the duration of the test
        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "before_broker_stop")

        node = self.kafka.leader(self.inputTopic)

        self.kafka.stop_node(node)

        time.sleep(broker_down_time_in_seconds)

        self.kafka.start_node(node)

        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "after_broker_stop",
                                    timeout_sec=120)

        self.kafka.stop()

    def test_streams_runs_with_broker_down_initially(self):
        self.kafka.start()
        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        configs = self.get_configs(extra_configs=",application.id=starting_wo_broker_id")

        # start streams with broker down initially
        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_3.start()

        broker_unavailable_message = "Broker may not be available"

        # verify streams instances unable to connect to broker, kept trying
        self.wait_for_verification(processor, broker_unavailable_message, processor.LOG_FILE, 100)
        self.wait_for_verification(processor_2, broker_unavailable_message, processor_2.LOG_FILE, 100)
        self.wait_for_verification(processor_3, broker_unavailable_message, processor_3.LOG_FILE, 100)

        # now start broker
        self.kafka.start_node(node)

        # assert streams can process when starting with broker down
        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "running_with_broker_down_initially",
                                    num_messages=9,
                                    timeout_sec=120)

        message = "processed3messages"
        # need to show all 3 instances processed messages
        self.wait_for_verification(processor, message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, message, processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, message, processor_3.STDOUT_FILE)

        self.kafka.stop()

    def test_streams_should_scale_in_while_brokers_down(self):
        self.kafka.start()

        configs = self.get_configs(extra_configs=",application.id=shutdown_with_broker_down")

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_3.start()

        # need to wait for rebalance once
        self.wait_for_verification(processor_3, "State transition from REBALANCING to RUNNING", processor_3.LOG_FILE)

        # assert streams can process when starting with broker up
        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "waiting for rebalance to complete",
                                    num_messages=9,
                                    timeout_sec=120)

        message = "processed3messages"

        self.wait_for_verification(processor, message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, message, processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, message, processor_3.STDOUT_FILE)

        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        processor.stop()
        processor_2.stop()

        shutdown_message = "Complete shutdown of streams resilience test app now"
        self.wait_for_verification(processor, shutdown_message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, shutdown_message, processor_2.STDOUT_FILE)

        self.kafka.start_node(node)

        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "sending_message_after_stopping_streams_instance_bouncing_broker",
                                    num_messages=9,
                                    timeout_sec=120)

        self.wait_for_verification(processor_3, "processed9messages", processor_3.STDOUT_FILE)

        self.kafka.stop()

    def test_streams_should_failover_while_brokers_down(self):
        self.kafka.start()

        configs = self.get_configs(extra_configs=",application.id=failover_with_broker_down")

        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor.start()

        processor_2 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_2.start()

        processor_3 = StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs)
        processor_3.start()

        # need to wait for rebalance once
        self.wait_for_verification(processor_3, "State transition from REBALANCING to RUNNING", processor_3.LOG_FILE)

        # assert streams can process when starting with broker up
        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "waiting for rebalance to complete",
                                    num_messages=9,
                                    timeout_sec=120)

        message = "processed3messages"

        self.wait_for_verification(processor, message, processor.STDOUT_FILE)
        self.wait_for_verification(processor_2, message, processor_2.STDOUT_FILE)
        self.wait_for_verification(processor_3, message, processor_3.STDOUT_FILE)

        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        processor.abortThenRestart()
        processor_2.abortThenRestart()
        processor_3.abortThenRestart()

        self.kafka.start_node(node)

        self.assert_produce_consume(self.inputTopic,
                                    self.outputTopic,
                                    self.client_id,
                                    "sending_message_after_hard_bouncing_streams_instance_bouncing_broker",
                                    num_messages=9,
                                    timeout_sec=120)

        self.kafka.stop()

이 코드는 Kafka Streams 장애 복원력을 검증한다는 테스트 목적 자체는 상당히 정확하지만, 테스트 코드의 장애 격리와 정리 보장이 약해서 테스트 자체가 실패했을 때 다음 테스트까지 오염시킬 가능성이 큰 레거시 통합 테스트다.

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

import time
from kafkatest.services.streams import StreamsBrokerDownResilienceService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest


class StreamsBrokerDownResilience(BaseStreamsTest):
    """
    This test validates that Streams is resilient to a broker
    being down longer than specified timeouts in configs
    """

    inputTopic = "streamsResilienceSource"
    outputTopic = "streamsResilienceSink"
    client_id = "streams-broker-resilience-verify-consumer"
    num_messages = 5

    def __init__(self, test_context):
        super(StreamsBrokerDownResilience, self).__init__(test_context,
                                                          topics={self.inputTopic: {'partitions': 3, 'replication-factor': 1},
                                                                  self.outputTopic: {'partitions': 1, 'replication-factor': 1}},
                                                          num_brokers=1)

    def setUp(self):
        self.zk.start()

    def _start_processors(self, configs, count=3):
        """Helper to create and start N processors cleanly without over-abstraction."""
        processors = [StreamsBrokerDownResilienceService(self.test_context, self.kafka, configs) for _ in range(count)]
        for p in processors:
            p.start()
        return processors

    def test_streams_resilient_to_broker_down(self):
        self.kafka.start()
        broker_down_time_in_seconds = 70
        processor = StreamsBrokerDownResilienceService(self.test_context, self.kafka, self.get_configs())
        
        processor.start()
        try:
            self.assert_produce_consume(self.inputTopic,
                                        self.outputTopic,
                                        self.client_id,
                                        "before_broker_stop")

            node = self.kafka.leader(self.inputTopic)
            self.kafka.stop_node(node)

            time.sleep(broker_down_time_in_seconds)

            self.kafka.start_node(node)

            self.assert_produce_consume(self.inputTopic,
                                        self.outputTopic,
                                        self.client_id,
                                        "after_broker_stop",
                                        timeout_sec=120)
        finally:
            # Guarantee Kafka and processor cleanup even if assertion fails
            processor.stop()
            self.kafka.stop()

    def test_streams_runs_with_broker_down_initially(self):
        self.kafka.start()
        node = self.kafka.leader(self.inputTopic)
        self.kafka.stop_node(node)

        configs = self.get_configs(extra_configs=",application.id=starting_wo_broker_id")
        processors = self._start_processors(configs, count=3)

        try:
            broker_unavailable_message = "Broker may not be available"
            for p in processors:
                self.wait_for_verification(p, broker_unavailable_message, p.LOG_FILE, 100)

            self.kafka.start_node(node)

            self.assert_produce_consume(self.inputTopic,
                                        self.outputTopic,
                                        self.client_id,
                                        "running_with_broker_down_initially",
                                        num_messages=9,
                                        timeout_sec=120)

            message = "processed3messages"
            for p in processors:
                self.wait_for_verification(p, message, p.STDOUT_FILE)
        finally:
            for p in processors:
                p.stop()
            self.kafka.stop()

    def test_streams_should_scale_in_while_brokers_down(self):
        self.kafka.start()
        configs = self.get_configs(extra_configs=",application.id=shutdown_with_broker_down")
        processors = self._start_processors(configs, count=3)
        processor, processor_2, processor_3 = processors

        try:
            self.wait_for_verification(processor_3, "State transition from REBALANCING to RUNNING", processor_3.LOG_FILE)

            self.assert_produce_consume(self.inputTopic,
                                        self.outputTopic,
                                        self.client_id,
                                        "waiting for rebalance to complete",
                                        num_messages=9,
                                        timeout_sec=120)

            message = "processed3messages"
            for p in processors:
                self.wait_for_verification(p, message, p.STDOUT_FILE)

            node = self.kafka.leader(self.inputTopic)
            self.kafka.stop_node(node)

            processor.stop()
            processor_2.stop()

            shutdown_message = "Complete shutdown of streams resilience test app now"
            self.wait_for_verification(processor, shutdown_message, processor.STDOUT_FILE)
            self.wait_for_verification(processor_2, shutdown_message, processor_2.STDOUT_FILE)

            self.kafka.start_node(node)

            self.assert_produce_consume(self.inputTopic,
                                        self.outputTopic,
                                        self.client_id,
                                        "sending_message_after_stopping_streams_instance_bouncing_broker",
                                        num_messages=9,
                                        timeout_sec=120)

            self.wait_for_verification(processor_3, "processed9messages", processor_3.STDOUT_FILE)
        finally:
            for p in processors:
                p.stop()
            self.kafka.stop()

    def test_streams_should_failover_while_brokers_down(self):
        self.kafka.start()
        configs = self.get_configs(extra_configs=",application.id=failover_with_broker_down")
        processors = self._start_processors(configs, count=3)
        processor, processor_2, processor_3 = processors

        try:
            self.wait_for_verification(processor_3, "State transition from REBALANCING to RUNNING", processor_3.LOG_FILE)

            self.assert_produce_consume(self.inputTopic,
                                        self.outputTopic,
                                        self.client_id,
                                        "waiting for rebalance to complete",
                                        num_messages=9,
                               5         timeout_sec=120)

            message = "processed3messages"
            for p in processors:
                self.wait_for_verification(p, message, p.STDOUT_FILE)

            node = self.kafka.leader(self.inputTopic)
            self.kafka.stop_node(node)

            for p in processors:
                p.abortThenRestart()

            self.kafka.start_node(node)

            self.assert_produce_consume(self.inputTopic,
                                        self.outputTopic,
                                        self.client_id,
                                        "sending_message_after_hard_bouncing_streams_instance_bouncing_broker",
                                        num_messages=9,
                                        timeout_sec=120)
        finally:
            for p in processors:
                p.stop()
            self.kafka.stop()

최종 개선사항
✅ 테스트 실패 시 cleanup 누락 → try/finally 기반 processor·Kafka 정리 보장 → 후속 테스트 환경 오염 방지
✅ 3개 Streams 인스턴스 생성 반복 → _start_processors()로 생성·기동 로직 통합 → 중복 제거와 테스트 의도 보존
✅ 개별 processor 검증 반복 → processors 순회 기반 검증 → 인스턴스 수 변경 시 유지보수성 확보
✅ Broker 장애 중 예외 발생 가능 → 테스트 종료 시 broker/process 자원 정리 → 통합 테스트의 장애 격리 강화
✅ 정상 종료와 강제 재시작 흐름이 혼재 → processor 목록을 단일 lifecycle로 관리 → 종료 상태 추적 및 코드 일관성 향상
✅ 테스트 종료 시 processor가 남을 가능성 → finally에서 모든 processor에 종료 명령 수행 → 테스트 프로세스 잔존 및 리소스 누수 방지

원본의 장애 복원력 검증 목적은 유지하면서 테스트 생명주기와 실패 복구를 강화했으며, 현재 버전은 중복을 줄이면서도 통합 테스트의 격리성과 운영 안정성을 확보한 구조다.
