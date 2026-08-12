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
from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig

import os
import re

TOPIC = "topic-consumer-group-command"


class ConsumerGroupCommandTest(Test):
    """
    Tests ConsumerGroupCommand
    """
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/consumer_group_command"
    COMMAND_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "command.properties")

    def __init__(self, test_context):
        super(ConsumerGroupCommandTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            TOPIC: {'partitions': 1, 'replication-factor': 1}
        }
        self.zk = ZookeeperService(test_context, self.num_zk)

    def setUp(self):
        self.zk.start()

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics)
        self.kafka.start()

    def start_consumer(self, security_protocol):
        enable_new_consumer = security_protocol == SecurityConfig.SSL
        self.consumer = ConsoleConsumer(self.test_context, num_nodes=self.num_brokers, kafka=self.kafka, topic=TOPIC,
                                        consumer_timeout_ms=None, new_consumer=enable_new_consumer)
        self.consumer.start()

    def setup_and_verify(self, security_protocol, group=None):
        self.start_kafka(security_protocol, security_protocol)
        self.start_consumer(security_protocol)
        consumer_node = self.consumer.nodes[0]
        wait_until(lambda: self.consumer.alive(consumer_node),
                   timeout_sec=10, backoff_sec=.2, err_msg="Consumer was too slow to start")
        kafka_node = self.kafka.nodes[0]
        if security_protocol is not SecurityConfig.PLAINTEXT:
            prop_file = str(self.kafka.security_config.client_config())
            self.logger.debug(prop_file)
            kafka_node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)
            kafka_node.account.create_file(self.COMMAND_CONFIG_FILE, prop_file)

        # Verify ConsumerGroupCommand lists expected consumer groups
        enable_new_consumer = security_protocol != SecurityConfig.PLAINTEXT
        command_config_file = None
        if enable_new_consumer:
            command_config_file = self.COMMAND_CONFIG_FILE

        if group:
            wait_until(lambda: re.search("topic-consumer-group-command",self.kafka.describe_consumer_group(group=group, node=kafka_node, new_consumer=enable_new_consumer, command_config=command_config_file)), timeout_sec=10,
                       err_msg="Timed out waiting to list expected consumer groups.")
        else:
            wait_until(lambda: "test-consumer-group" in self.kafka.list_consumer_groups(node=kafka_node, new_consumer=enable_new_consumer, command_config=command_config_file), timeout_sec=10,
                       err_msg="Timed out waiting to list expected consumer groups.")

        self.consumer.stop()

    @cluster(num_nodes=3)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    def test_list_consumer_groups(self, security_protocol='PLAINTEXT'):
        """
        Tests if ConsumerGroupCommand is listing correct consumer groups
        :return: None
        """
        self.setup_and_verify(security_protocol)

    @cluster(num_nodes=3)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    def test_describe_consumer_group(self, security_protocol='PLAINTEXT'):
        """
        Tests if ConsumerGroupCommand is describing a consumer group correctly
        :return: None
        """
        self.setup_and_verify(security_protocol, group="test-consumer-group")

원본 코드의 핵심 기능과 테스트 매트릭스는 탄탄하지만, 검증 대상과 assertion 조건이 어긋나고 실패 시 cleanup이 보장되지 않아 테스트 자체의 신뢰성을 흔들 수 있는 구조다.

제안패치
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.utils.util import wait_until
from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig

import os
import re
import inspect

TOPIC = "topic-consumer-group-command"
TARGET_GROUP = "test-consumer-group"


class ConsumerGroupCommandTest(Test):
    """
    Tests ConsumerGroupCommand with enterprise-grade defensive exception handling,
    strict API contract verification, and robust cleanup management.
    """
    PERSISTENT_ROOT = "/mnt/consumer_group_command"
    COMMAND_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "command.properties")

    def __init__(self, test_context):
        super(ConsumerGroupCommandTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            TOPIC: {'partitions': 1, 'replication-factor': 1}
        }
        self.zk = ZookeeperService(test_context, self.num_zk)
        self.kafka = None
        self.consumer = None

    def setUp(self):
        self.zk.start()

    def _is_new_consumer(self, security_protocol):
        """
        [지적 6번 반영] enable_new_consumer 조건식 파편화 방지 (단일화)
        PLAINTEXT 외 보안 프로토콜(SSL 등) 확장 시 일관성 유지
        """
        return security_protocol != SecurityConfig.PLAINTEXT

    def _start_kafka(self, security_protocol):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            self.zk, security_protocol=security_protocol,
            interbroker_security_protocol=security_protocol, topics=self.topics)
        self.kafka.start()

    def _start_consumer(self, security_protocol):
        enable_new_consumer = self._is_new_consumer(security_protocol)
        
        # [지적 1번 반영] ConsoleConsumer API 계약 검증 및 동적 인자 주입
        consumer_kwargs = {
            "test_context": self.test_context,
            "num_nodes": self.num_brokers,
            "kafka": self.kafka,
            "topic": TOPIC,
            "consumer_timeout_ms": None,
            "new_consumer": enable_new_consumer
        }
        
        # 실제 ConsoleConsumer 생성자가 group_id 인자를 지원하는지 signature 동적 확인
        sig = inspect.signature(ConsoleConsumer.__init__)
        if 'group_id' in sig.parameters:
            consumer_kwargs['group_id'] = TARGET_GROUP

        self.consumer = ConsoleConsumer(**consumer_kwargs)
        self.consumer.start()
        
        consumer_node = self.consumer.nodes[0]
        wait_until(
            lambda: self.consumer.alive(consumer_node),
            timeout_sec=10, backoff_sec=0.2,
            err_msg="Consumer was too slow to start"
        )

    def _setup_security_config(self, security_protocol):
        if security_protocol != SecurityConfig.PLAINTEXT:
            kafka_node = self.kafka.nodes[0]
            prop_file = str(self.kafka.security_config.client_config())
            self.logger.debug(prop_file)
            kafka_node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)
            kafka_node.account.create_file(self.COMMAND_CONFIG_FILE, prop_file)

    def setup_and_verify(self, security_protocol, group=None):
        """
        [지적 2번 반영] 테스트 본래의 예외를 덮어쓰지 않고, 
        Cleanup 실패와 분리하여 로깅하는 방어적 try-finally 구조
        """
        test_failed = True
        try:
            self._start_kafka(security_protocol)
            self._start_consumer(security_protocol)
            self._setup_security_config(security_protocol)

            kafka_node = self.kafka.nodes[0]
            enable_new_consumer = self._is_new_consumer(security_protocol)
            command_config_file = self.COMMAND_CONFIG_FILE if enable_new_consumer else None

            if group:
                # [지적 3번 반영] 그룹 ID뿐만 아니라 출력 결과의 구조적 무결성(파트너십/오프셋 정보 등) 함께 검증
                def verify_description():
                    desc_output = self.kafka.describe_consumer_group(
                        group=group, node=kafka_node,
                        new_consumer=enable_new_consumer,
                        command_config=command_config_file
                    )
                    if not desc_output:
                        return False
                    # 그룹 이름 매칭 및 카프카 ConsumerGroupCommand 표준 출력 키워드(PARTITION 또는 CURRENT-OFFSET 등) 검증
                    has_group = bool(re.search(r"\b%s\b" % re.escape(group), desc_output))
                    has_structure = ("PARTITION" in desc_output) or ("CURRENT-OFFSET" in desc_output) or (TOPIC in desc_output)
                    return has_group and has_structure

                wait_until(
                    verify_description,
                    timeout_sec=10,
                    err_msg="Timed out waiting to describe consumer group with structural integrity: %s" % group
                )
            else:
                wait_until(
                    lambda: TARGET_GROUP in self.kafka.list_consumer_groups(
                        node=kafka_node, new_consumer=enable_new_consumer,
                        command_config=command_config_file
                    ),
                    timeout_sec=10,
                    err_msg="Timed out waiting to list expected consumer groups including '%s'." % TARGET_GROUP
                )
            test_failed = False
        finally:
            if self.consumer:
                try:
                    self.consumer.stop()
                except Exception as cleanup_err:
                    if test_failed:
                        self.logger.error("Consumer cleanup failed while test was already failing: %s" % str(cleanup_err))
                    else:
                        raise RuntimeError("Consumer cleanup failed: %s" % str(cleanup_err))

    @cluster(num_nodes=3)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    def test_list_consumer_groups(self, security_protocol='PLAINTEXT'):
        """
        Tests if ConsumerGroupCommand is listing correct consumer groups
        :return: None
        """
        self.setup_and_verify(security_protocol)

    @cluster(num_nodes=3)
    @matrix(security_protocol=['PLAINTEXT', 'SSL'])
    def test_describe_consumer_group(self, security_protocol='PLAINTEXT'):
        """
        Tests if ConsumerGroupCommand is describing a consumer group correctly
        :return: None
        """
        self.setup_and_verify(security_protocol, group=TARGET_GROUP)

최종 개선사항
✅ 컨슈머 그룹 ID 무조건 주입 → ConsoleConsumer.__init__ 시그니처 확인 후 지원 시에만 주입 → 기존 API 호환성을 보존하면서 그룹 검증 대상 고정
✅ 보안 프로토콜별 enable_new_consumer 조건 분산 → _is_new_consumer()로 단일화 → 프로토콜 확장 시 조건 불일치 방지
✅ 그룹명 문자열만 확인 → 그룹명 + ConsumerGroupCommand 출력 구조 키워드 동시 검증 → 잘못된 출력의 우연한 통과 가능성 축소
✅ 테스트 실패 시 cleanup 예외 무시 → 원래 실패와 cleanup 실패를 분리 처리 → 장애 원인 은폐 방지 및 실패 정보 보존
✅ 보안 설정 준비 로직과 테스트 본체 결합 → _setup_security_config()로 책임 분리 → 환경 구성과 검증 로직의 유지보수성 향상
✅ describe_consumer_group 검증 조건 단순화 → verify_description() 기반 재시도 가능한 검증 함수로 분리 → 비동기 CLI 결과에 대한 판정 일관성 확보

✅ 원본의 PLAINTEXT/SSL 테스트 의미 유지 → 필요한 방어층만 추가하고 Kafka/ZooKeeper lifecycle을 임의 변경하지 않음 → 기존 DuccTape 테스트 계약과 회귀 가능성 최소화        
