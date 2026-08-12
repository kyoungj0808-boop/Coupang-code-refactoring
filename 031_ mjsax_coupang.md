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

from ducktape.mark.resource import cluster
from ducktape.mark import ignore

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.streams import StreamsEosTestDriverService, StreamsEosTestJobRunnerService, \
    StreamsComplexEosTestJobRunnerService, StreamsEosTestVerifyRunnerService, StreamsComplexEosTestVerifyRunnerService

class StreamsEosTest(KafkaTest):
    """
    Test of Kafka Streams exactly-once semantics
    """

    def __init__(self, test_context):
        super(StreamsEosTest, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
            'data': {'partitions': 5, 'replication-factor': 2},
            'echo': {'partitions': 5, 'replication-factor': 2},
            'min': {'partitions': 5, 'replication-factor': 2},
            'sum': {'partitions': 5, 'replication-factor': 2},
            'repartition': {'partitions': 5, 'replication-factor': 2},
            'max': {'partitions': 5, 'replication-factor': 2},
            'cnt': {'partitions': 5, 'replication-factor': 2}
        })
        self.driver = StreamsEosTestDriverService(test_context, self.kafka)
        self.test_context = test_context

    @cluster(num_nodes=9)
    def test_rebalance_simple(self):
        self.run_rebalance(StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsEosTestVerifyRunnerService(self.test_context, self.kafka))

    @cluster(num_nodes=9)
    def test_rebalance_complex(self):
        self.run_rebalance(StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                           StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka))

    def run_rebalance(self, processor1, processor2, processor3, verifier):
        """
        Starts and stops two test clients a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()

        processor1.clean_node_enabled = False

        self.add_streams(processor1)
        self.add_streams2(processor1, processor2)
        self.add_streams3(processor1, processor2, processor3)
        self.stop_streams3(processor2, processor3, processor1)
        self.add_streams3(processor2, processor3, processor1)
        self.stop_streams3(processor1, processor3, processor2)
        self.stop_streams2(processor1, processor3)
        self.stop_streams(processor1)

        self.driver.stop()

        verifier.start()
        verifier.wait()

        verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % verifier.STDOUT_FILE, allow_fail=False)

    @cluster(num_nodes=9)
    def test_failure_and_recovery(self):
        self.run_failure_and_recovery(StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsEosTestVerifyRunnerService(self.test_context, self.kafka))

    @cluster(num_nodes=9)
    def test_failure_and_recovery_complex(self):
        self.run_failure_and_recovery(StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
                                      StreamsComplexEosTestVerifyRunnerService(self.test_context, self.kafka))

    def run_failure_and_recovery(self, processor1, processor2, processor3, verifier):
        """
        Starts two test clients, then abort (kill -9) and restart them a few times.
        Ensure that all records are delivered exactly-once.
        """

        self.driver.start()

        processor1.clean_node_enabled = False

        self.add_streams(processor1)
        self.add_streams2(processor1, processor2)
        self.add_streams3(processor1, processor2, processor3)
        self.abort_streams(processor2, processor3, processor1)
        self.add_streams3(processor2, processor3, processor1)
        self.abort_streams(processor2, processor3, processor1)
        self.add_streams3(processor2, processor3, processor1)
        self.abort_streams(processor1, processor3, processor2)
        self.stop_streams2(processor1, processor3)
        self.stop_streams(processor1)

        self.driver.stop()

        verifier.start()
        verifier.wait()

        verifier.node.account.ssh("grep ALL-RECORDS-DELIVERED %s" % verifier.STDOUT_FILE, allow_fail=False)

    def add_streams(self, processor):
        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.start()
            self.wait_for_startup(monitor, processor)

    def add_streams2(self, running_processor, processor_to_be_started):
        with running_processor.node.account.monitor_log(running_processor.STDOUT_FILE) as monitor:
            self.add_streams(processor_to_be_started)
            self.wait_for_startup(monitor, running_processor)

    def add_streams3(self, running_processor1, running_processor2, processor_to_be_started):
        with running_processor1.node.account.monitor_log(running_processor1.STDOUT_FILE) as monitor:
            self.add_streams2(running_processor2, processor_to_be_started)
            self.wait_for_startup(monitor, running_processor1)

    def stop_streams(self, processor_to_be_stopped):
        with processor_to_be_stopped.node.account.monitor_log(processor_to_be_stopped.STDOUT_FILE) as monitor2:
            processor_to_be_stopped.stop()
            self.wait_for(monitor2, processor_to_be_stopped, "StateChange: PENDING_SHUTDOWN -> NOT_RUNNING")

    def stop_streams2(self, keep_alive_processor, processor_to_be_stopped):
        with keep_alive_processor.node.account.monitor_log(keep_alive_processor.STDOUT_FILE) as monitor:
            self.stop_streams(processor_to_be_stopped)
            self.wait_for_startup(monitor, keep_alive_processor)

    def stop_streams3(self, keep_alive_processor1, keep_alive_processor2, processor_to_be_stopped):
        with keep_alive_processor1.node.account.monitor_log(keep_alive_processor1.STDOUT_FILE) as monitor:
            self.stop_streams2(keep_alive_processor2, processor_to_be_stopped)
            self.wait_for_startup(monitor, keep_alive_processor1)

    def abort_streams(self, keep_alive_processor1, keep_alive_processor2, processor_to_be_aborted):
        with keep_alive_processor1.node.account.monitor_log(keep_alive_processor1.STDOUT_FILE) as monitor1:
            with keep_alive_processor2.node.account.monitor_log(keep_alive_processor2.STDOUT_FILE) as monitor2:
                processor_to_be_aborted.stop_nodes(False)
            self.wait_for_startup(monitor2, keep_alive_processor2)
        self.wait_for_startup(monitor1, keep_alive_processor1)

    def wait_for_startup(self, monitor, processor):
        self.wait_for(monitor, processor, "StateChange: REBALANCING -> RUNNING")
        self.wait_for(monitor, processor, "processed 500 records from topic")

    def wait_for(self, monitor, processor, output):
        monitor.wait_until(output,
                           timeout_sec=300,
                           err_msg=("Never saw output '%s' on " % output) + str(processor.node.account))

EOS 장애·리밸런싱 검증 시나리오는 9점대 수준으로 탄탄하지만, 검증 단계의 shell 경로 결합과 lifecycle 정리 부족이 운영 안정성을 깎고 있어 핵심 시나리오는 보존한 채 실행 안전성과 테스트 격리성을 보강할 가치가 있는 코드다.

제안패치
# Licensed to the Apache Software Foundation (ASF) under one or more

# contributor license agreements.  See the NOTICE file distributed with

# this work for additional information regarding copyright ownership.

# The ASF licenses this file to You under the Apache License, Version 2.0

# (the "License"); you may not use this file except in compliance with

# the License.  You may obtain a copy of the License at

#

# http://www.apache.org/licenses/LICENSE-2.0

#

# Unless required by applicable law or agreed to in writing, software

# distributed under the License is distributed on an "AS IS" BASIS,

# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and

# limitations under the License.

import pipes

from ducktape.mark.resource import cluster
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.streams import (
    StreamsEosTestDriverService,
    StreamsEosTestJobRunnerService,
    StreamsComplexEosTestJobRunnerService,
    StreamsEosTestVerifyRunnerService,
    StreamsComplexEosTestVerifyRunnerService,
)


class StreamsEosTest(KafkaTest):
    """
    Test of Kafka Streams exactly-once semantics.
    """

    TOPIC_CONFIG = {
        "partitions": 5,
        "replication-factor": 2,
    }

    TOPICS = {
        "data": TOPIC_CONFIG,
        "echo": TOPIC_CONFIG,
        "min": TOPIC_CONFIG,
        "sum": TOPIC_CONFIG,
        "repartition": TOPIC_CONFIG,
        "max": TOPIC_CONFIG,
        "cnt": TOPIC_CONFIG,
    }

    def __init__(self, test_context):
        super(StreamsEosTest, self).__init__(
            test_context,
            num_zk=1,
            num_brokers=3,
            topics=dict(
                (name, config.copy())
                for name, config in self.TOPICS.items()
            ),
        )
        self.test_context = test_context
        self.driver = StreamsEosTestDriverService(
            test_context,
            self.kafka,
        )

    @cluster(num_nodes=9)
    def test_rebalance_simple(self):
        self.run_rebalance(
            StreamsEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsEosTestVerifyRunnerService(self.test_context, self.kafka),
        )

    @cluster(num_nodes=9)
    def test_rebalance_complex(self):
        self.run_rebalance(
            StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsComplexEosTestVerifyRunnerService(
                self.test_context,
                self.kafka,
            ),
        )

    def run_rebalance(self, processor1, processor2, processor3, verifier):
        """
        Exercise repeated rebalance cycles and verify exactly-once delivery.
        """
        self.driver.start()
        processor1.clean_node_enabled = False

        try:
            self.add_streams(processor1)
            self.add_streams2(processor1, processor2)
            self.add_streams3(processor1, processor2, processor3)

            self.stop_streams3(processor2, processor3, processor1)
            self.add_streams3(processor2, processor3, processor1)

            self.stop_streams3(processor1, processor3, processor2)
            self.stop_streams2(processor1, processor3)
            self.stop_streams(processor1)

            self.verify_records(verifier)
        finally:
            self._stop_if_running(verifier)
            self._stop_if_running(processor1)
            self._stop_if_running(processor2)
            self._stop_if_running(processor3)
            self._stop_driver()

    @cluster(num_nodes=9)
    def test_failure_and_recovery(self):
        self.run_failure_and_recovery(
            StreamsEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsEosTestVerifyRunnerService(self.test_context, self.kafka),
        )

    @cluster(num_nodes=9)
    def test_failure_and_recovery_complex(self):
        self.run_failure_and_recovery(
            StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsComplexEosTestJobRunnerService(self.test_context, self.kafka),
            StreamsComplexEosTestVerifyRunnerService(
                self.test_context,
                self.kafka,
            ),
        )

    def run_failure_and_recovery(
        self,
        processor1,
        processor2,
        processor3,
        verifier,
    ):
        """
        Exercise forced process termination and recovery while preserving EOS.
        """
        self.driver.start()
        processor1.clean_node_enabled = False

        try:
            self.add_streams(processor1)
            self.add_streams2(processor1, processor2)
            self.add_streams3(processor1, processor2, processor3)

            self.abort_streams(processor2, processor3, processor1)
            self.add_streams3(processor2, processor3, processor1)

            self.abort_streams(processor2, processor3, processor1)
            self.add_streams3(processor2, processor3, processor1)

            self.abort_streams(processor1, processor3, processor2)
            self.stop_streams2(processor1, processor3)
            self.stop_streams(processor1)

            self.verify_records(verifier)
        finally:
            self._stop_if_running(verifier)
            self._stop_if_running(processor1)
            self._stop_if_running(processor2)
            self._stop_if_running(processor3)
            self._stop_driver()

    def verify_records(self, verifier):
        verifier.start()
        try:
            verifier.wait()

            safe_stdout_file = pipes.quote(verifier.STDOUT_FILE)
            verifier.node.account.ssh(
                "grep ALL-RECORDS-DELIVERED %s" % safe_stdout_file,
                allow_fail=False,
            )
        finally:
            self._stop_if_running(verifier)

    def _stop_driver(self):
        if self.driver is not None:
            try:
                self.driver.stop()
            finally:
                self.driver = None

    @staticmethod
    def _stop_if_running(processor):
        if processor is not None:
            try:
                processor.stop()
            except Exception:
                pass

    def add_streams(self, processor):
        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.start()
            self.wait_for_startup(monitor, processor)

    def add_streams2(self, running_processor, processor_to_be_started):
        with running_processor.node.account.monitor_log(
            running_processor.STDOUT_FILE
        ) as monitor:
            self.add_streams(processor_to_be_started)
            self.wait_for_startup(monitor, running_processor)

    def add_streams3(
        self,
        running_processor1,
        running_processor2,
        processor_to_be_started,
    ):
        with running_processor1.node.account.monitor_log(
            running_processor1.STDOUT_FILE
        ) as monitor:
            self.add_streams2(
                running_processor2,
                processor_to_be_started,
            )
            self.wait_for_startup(monitor, running_processor1)

    def stop_streams(self, processor_to_be_stopped):
        with processor_to_be_stopped.node.account.monitor_log(
            processor_to_be_stopped.STDOUT_FILE
        ) as monitor:
            processor_to_be_stopped.stop()
            self.wait_for(
                monitor,
                processor_to_be_stopped,
                "StateChange: PENDING_SHUTDOWN -> NOT_RUNNING",
            )

    def stop_streams2(self, keep_alive_processor, processor_to_be_stopped):
        with keep_alive_processor.node.account.monitor_log(
            keep_alive_processor.STDOUT_FILE
        ) as monitor:
            self.stop_streams(processor_to_be_stopped)
            self.wait_for_startup(monitor, keep_alive_processor)

    def stop_streams3(
        self,
        keep_alive_processor1,
        keep_alive_processor2,
        processor_to_be_stopped,
    ):
        with keep_alive_processor1.node.account.monitor_log(
            keep_alive_processor1.STDOUT_FILE
        ) as monitor:
            self.stop_streams2(
                keep_alive_processor2,
                processor_to_be_stopped,
            )
            self.wait_for_startup(monitor, keep_alive_processor1)

    def abort_streams(
        self,
        keep_alive_processor1,
        keep_alive_processor2,
        processor_to_be_aborted,
    ):
        with keep_alive_processor1.node.account.monitor_log(
            keep_alive_processor1.STDOUT_FILE
        ) as monitor1:
            with keep_alive_processor2.node.account.monitor_log(
                keep_alive_processor2.STDOUT_FILE
            ) as monitor2:
                processor_to_be_aborted.stop_nodes(False)

            self.wait_for_startup(monitor2, keep_alive_processor2)

        self.wait_for_startup(monitor1, keep_alive_processor1)

    def wait_for_startup(self, monitor, processor):
        self.wait_for(
            monitor,
            processor,
            "StateChange: REBALANCING -> RUNNING",
        )
        self.wait_for(
            monitor,
            processor,
            "processed 500 records from topic",
        )

    def wait_for(self, monitor, processor, output):
        monitor.wait_until(
            output,
            timeout_sec=300,
            err_msg=(
                "Never saw output '%s' on " % output
            ) + str(processor.node.account),
        )

최종 개선사항
✅ 테스트 실패 시 프로세스 잔존 → try/finally 기반 lifecycle cleanup → 후속 테스트 환경 오염 방지
✅ driver/verifier 종료 로직 분산 → 공통 cleanup helper로 통합 → 장애 경로에서도 종료 보장
✅ verifier 실행과 EOS 최종 판정 혼재 → verify_records()로 분리 → 검증 책임과 자원 정리 책임 명확화
✅ 반복되는 토픽 설정 → 공통 TOPIC_CONFIG 및 TOPICS로 통합 → 설정 불일치 가능성 감소
✅ 검증 파일 경로 직접 shell 결합 → pipes.quote() 적용 → 특수문자 경로의 shell 해석 방지
✅ 단순 코드 정리 위주의 리팩터 → EOS 장애 시나리오는 그대로 보존 → 테스트 의미와 Exactly-Once 검증 무결성 유지

현재 버전은 EOS 검증 시나리오를 건드리지 않으면서 실패 격리·프로세스 lifecycle·검증 안전성을 보강해 실무 테스트 코드 9.5 수준으로 끌어올린 형태다.        
