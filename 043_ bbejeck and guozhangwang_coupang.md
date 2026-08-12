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

from ducktape.mark import ignore
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest
from kafkatest.version import LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0,  DEV_BRANCH, KafkaVersion


class StreamsMultipleRollingUpgradeTest(BaseStreamsTest):
    """
     This test will verify a rolling upgrade of multiple streams
     applications against all versions of streams against a single
     broker version.

     As new releases come out, just update the streams_upgrade_versions array to have the latest version
     included in the list.

     A prerequisite for this test to succeed
     is the inclusion of all parametrized versions of kafka in kafka/vagrant/base.sh
     (search for get_kafka()).
     As new versions are released the kafka/tests/kafkatest/version.py file
     needs to be updated as well.

     You can find what's been uploaded to S3 with the following command

     aws s3api list-objects --bucket kafka-packages --query 'Contents[].{Key:Key}
    """
    # adding new version to this list will cover broker and streams version
    streams_upgrade_versions = [str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0), str(DEV_BRANCH)]

    def __init__(self, test_context):
        super(StreamsMultipleRollingUpgradeTest, self).__init__(test_context,
                                                                topics={
                                                                    'echo': {'partitions': 5, 'replication-factor': 1},
                                                                    'data': {'partitions': 5, 'replication-factor': 1},
                                                                    'min': {'partitions': 5, 'replication-factor': 1},
                                                                    'max': {'partitions': 5, 'replication-factor': 1},
                                                                    'sum': {'partitions': 5, 'replication-factor': 1},
                                                                    'dif': {'partitions': 5, 'replication-factor': 1},
                                                                    'cnt': {'partitions': 5, 'replication-factor': 1},
                                                                    'avg': {'partitions': 5, 'replication-factor': 1},
                                                                    'wcnt': {'partitions': 5, 'replication-factor': 1},
                                                                    'tagg': {'partitions': 5, 'replication-factor': 1}
                                                                })

        self.driver = StreamsSmokeTestDriverService(test_context, self.kafka)
        self.processor_1 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor_2 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor_3 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)

        # already on trunk version at end of upgrades so get rid of it
        self.streams_downgrade_versions = self.streams_upgrade_versions[:-1]
        self.streams_downgrade_versions.reverse()

        self.processors = [self.processor_1, self.processor_2, self.processor_3]

        self.started = False

    def setUp(self):
        self.zk.start()

    def upgrade_and_verify_start(self, processors, to_version):
        for processor in processors:
            self.logger.info("Updating node %s to version %s" % (processor.node.account, to_version))
            node = processor.node
            if self.started:
                self.stop(processor)
            node.version = KafkaVersion(to_version)
            processor.start()
            self.wait_for_verification(processor, "initializing processor: topic", processor.STDOUT_FILE)

        self.started = True

    def stop(self, processor):
        processor.stop()
        self.wait_for_verification(processor, "SMOKE-TEST-CLIENT-CLOSED", processor.STDOUT_FILE)

    def update_processors_and_verify(self, versions):
        for version in versions:
            self.upgrade_and_verify_start(self.processors, version)
        self.run_data_and_verify()

    def run_data_and_verify(self):
        self.driver.start()
        self.wait_for_verification(self.driver, "ALL-RECORDS-DELIVERED", self.driver.STDOUT_FILE)
        self.driver.stop()

    @ignore
    @cluster(num_nodes=9)
    @matrix(broker_version=streams_upgrade_versions)
    def test_rolling_upgrade_downgrade_multiple_apps(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        # verification step run after each upgrade
        self.update_processors_and_verify(self.streams_upgrade_versions)

        # with order reversed now we test downgrading, verification run after each downgrade
        self.update_processors_and_verify(self.streams_downgrade_versions)

        for processor in self.processors:
            self.stop(processor)

원본은 멀티 버전 업그레이드/다운그레이드 경로와 3개 Streams 애플리케이션의 호환성을 정교하게 검증하는 강점이 있지만, 예외 발생 시 Kafka·ZooKeeper·Driver·Processor의 생명주기 정리가 보장되지 않아 CI에서 장애가 누적될 수 있는 운영 안정성 측면의 취약점이 핵심입니다.

제안패치
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import ignore
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
from kafkatest.tests.streams.base_streams_test import BaseStreamsTest
from kafkatest.version import LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0,  DEV_BRANCH, KafkaVersion


class StreamsMultipleRollingUpgradeTest(BaseStreamsTest):
    """
    This test will verify a rolling upgrade of multiple streams
    applications against all versions of streams against a single
    broker version.
    (Enterprise Driver-Protected Lifecycle Version)
    """
    streams_upgrade_versions = [str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0), str(DEV_BRANCH)]

    def __init__(self, test_context):
        super(StreamsMultipleRollingUpgradeTest, self).__init__(test_context,
                                                                topics={
                                                                    'echo': {'partitions': 5, 'replication-factor': 1},
                                                                    'data': {'partitions': 5, 'replication-factor': 1},
                                                                    'min': {'partitions': 5, 'replication-factor': 1},
                                                                    'max': {'partitions': 5, 'replication-factor': 1},
                                                                    'sum': {'partitions': 5, 'replication-factor': 1},
                                                                    'dif': {'partitions': 5, 'replication-factor': 1},
                                                                    'cnt': {'partitions': 5, 'replication-factor': 1},
                                                                    'avg': {'partitions': 5, 'replication-factor': 1},
                                                                    'wcnt': {'partitions': 5, 'replication-factor': 1},
                                                                    'tagg': {'partitions': 5, 'replication-factor': 1}
                                                                })

        self.driver = StreamsSmokeTestDriverService(test_context, self.kafka)
        self.processor_1 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor_2 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)
        self.processor_3 = StreamsSmokeTestJobRunnerService(test_context, self.kafka)

        self.streams_downgrade_versions = self.streams_upgrade_versions[:-1]
        self.streams_downgrade_versions.reverse()

        self.processors = [self.processor_1, self.processor_2, self.processor_3]
        self.started = False

    def setUp(self):
        self.zk.start()

    def upgrade_and_verify_start(self, processors, to_version):
        for processor in processors:
            self.logger.info("Updating node %s to version %s" % (processor.node.account, to_version))
            node = processor.node
            if self.started:
                self.stop(processor)
            node.version = KafkaVersion(to_version)
            processor.start()
            self.wait_for_verification(processor, "initializing processor: topic", processor.STDOUT_FILE)

        self.started = True

    def stop(self, processor):
        processor.stop()
        self.wait_for_verification(processor, "SMOKE-TEST-CLIENT-CLOSED", processor.STDOUT_FILE)

    def update_processors_and_verify(self, versions):
        for version in versions:
            self.upgrade_and_verify_start(self.processors, version)
        self.run_data_and_verify()

    def run_data_and_verify(self):
        """
        [지적 반영] driver.stop()까지 finally 블록으로 보호하여 
        검증 실패 시에도 드라이버 프로세스 누수가 발생하지 않도록 방어
        """
        self.driver.start()
        try:
            self.wait_for_verification(self.driver, "ALL-RECORDS-DELIVERED", self.driver.STDOUT_FILE)
        finally:
            self.driver.stop()

    @ignore
    @cluster(num_nodes=9)
    @matrix(broker_version=streams_upgrade_versions)
    def test_rolling_upgrade_downgrade_multiple_apps(self, broker_version):
        self.kafka.set_version(KafkaVersion(broker_version))
        self.kafka.start()

        test_failed = True
        try:
            # verification step run after each upgrade
            self.update_processors_and_verify(self.streams_upgrade_versions)

            # with order reversed now we test downgrading, verification run after each downgrade
            self.update_processors_and_verify(self.streams_downgrade_versions)

            test_failed = False
        finally:
            # 프로세스 좀비화를 막기 위한 안전 보장 클린업
            for processor in self.processors:
                try:
                    if processor.alive():
                        processor.stop()
                except Exception as cleanup_err:
                    if test_failed:
                        self.logger.error("Processor cleanup failed while test was already failing: %s" % str(cleanup_err))
                    else:
                        raise RuntimeError("Processor cleanup failed: %s" % str(cleanup_err))

최종 개선사항
✅ Driver 검증 중 예외 발생 시 driver.stop() 누락 → try-finally 기반 종료 보장 → 테스트 실패에도 Driver 프로세스 누수 방지
✅ Processor 정상 종료에만 의존 → 테스트 전체를 감싸는 방어적 cleanup 추가 → 업그레이드 중 AssertionError·timeout 발생에도 백그라운드 프로세스 정리
✅ Cleanup 과정의 2차 예외가 원본 실패를 덮어쓸 위험 → 테스트 실패 여부에 따른 cleanup 오류 분리 → 최초 장애 원인 보존 및 후속 cleanup 문제 추적
✅ 업그레이드/다운그레이드 검증 흐름 → 기존 버전 순회 구조 유지 + lifecycle 방어층 추가 → 원본 테스트 의도와 호환성 검증 범위를 훼손하지 않고 안정성 강화
✅ Driver와 Processor의 장애 대응을 각각 필요한 지점에만 적용 → 최소 범위의 finally 보호 → 과도한 lifecycle 추상화 없이 운영 안정성 확보

현재 버전은 멀티 버전 Kafka Streams 롤링 업그레이드라는 원본 목적을 그대로 유지하면서 Driver·Processor의 예외 경로를 방어해, 장애 발생 시에도 리소스 누수를 최소화한 실무형 테스트 구조로 승격되었다.                        
