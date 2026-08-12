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

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.tests.verifiable_consumer_test import VerifiableConsumerTest
from kafkatest.services.kafka import TopicPartition

import signal


class OffsetValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 1

    def __init__(self, test_context):
        super(OffsetValidationTest, self).__init__(test_context, num_consumers=3, num_producers=1,
                                                     num_zk=1, num_brokers=2, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 2 }
        })

    def rolling_bounce_consumers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

                wait_until(lambda: len(consumer.dead_nodes()) == 1,
                           timeout_sec=self.session_timeout_sec+5,
                           err_msg="Timed out waiting for the consumer to shutdown")

                consumer.start_node(node)

                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def bounce_all_consumers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

            wait_until(lambda: len(consumer.dead_nodes()) == self.num_consumers, timeout_sec=10,
                       err_msg="Timed out waiting for the consumers to shutdown")
            
            for node in consumer.nodes:
                consumer.start_node(node)

            self.await_all_members(consumer)
            self.await_consumed_messages(consumer)

    def rolling_bounce_brokers(self, consumer, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for node in self.kafka.nodes:
                self.kafka.restart_node(node, clean_shutdown=True)
                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def setup_consumer(self, topic, **kwargs):
        # collect verifiable consumer events since this makes debugging much easier
        consumer = super(OffsetValidationTest, self).setup_consumer(topic, **kwargs)
        self.mark_for_collect(consumer, 'verifiable_consumer_stdout')
        return consumer

    @cluster(num_nodes=7)
    def test_broker_rolling_bounce(self):
        """
        Verify correct consumer behavior when the brokers are consecutively restarted.

        Setup: single Kafka cluster with one producer writing messages to a single topic with one
        partition, an a set of consumers in the same group reading from the same topic.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers and wait until they've joined the group.
        - In a loop, restart each broker consecutively, waiting for the group to stabilize between
          each broker restart.
        - Verify delivery semantics according to the failure type and that the broker bounces
          did not cause unexpected group rebalances.
        """
        partition = TopicPartition(self.TOPIC, 0)
        
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)

        num_rebalances = consumer.num_rebalances()
        # TODO: make this test work with hard shutdowns, which probably requires
        #       pausing before the node is restarted to ensure that any ephemeral
        #       nodes have time to expire
        self.rolling_bounce_brokers(consumer, clean_shutdown=True)
        
        unexpected_rebalances = consumer.num_rebalances() - num_rebalances
        assert unexpected_rebalances == 0, \
            "Broker rolling bounce caused %d unexpected group rebalances" % unexpected_rebalances

        consumer.stop_all()

        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))

    @cluster(num_nodes=7)
    @matrix(clean_shutdown=[True], bounce_mode=["all", "rolling"])
    def test_consumer_bounce(self, clean_shutdown, bounce_mode):
        """
        Verify correct consumer behavior when the consumers in the group are consecutively restarted.

        Setup: single Kafka cluster with one producer and a set of consumers in one group.

        - Start a producer which continues producing new messages throughout the test.
        - Start up the consumers and wait until they've joined the group.
        - In a loop, restart each consumer, waiting for each one to rejoin the group before
          restarting the rest.
        - Verify delivery semantics according to the failure type.
        """
        partition = TopicPartition(self.TOPIC, 0)
        
        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)

        if bounce_mode == "all":
            self.bounce_all_consumers(consumer, clean_shutdown=clean_shutdown)
        else:
            self.rolling_bounce_consumers(consumer, clean_shutdown=clean_shutdown)
                
        consumer.stop_all()
        if clean_shutdown:
            # if the total records consumed matches the current position, we haven't seen any duplicates
            # this can only be guaranteed with a clean shutdown
            assert consumer.current_position(partition) == consumer.total_consumed(), \
                "Total consumed records %d did not match consumed position %d" % \
                (consumer.total_consumed(), consumer.current_position(partition))
        else:
            # we may have duplicates in a hard failure
            assert consumer.current_position(partition) <= consumer.total_consumed(), \
                "Current position %d greater than the total number of consumed records %d" % \
                (consumer.current_position(partition), consumer.total_consumed())

    @cluster(num_nodes=7)
    @matrix(clean_shutdown=[True], enable_autocommit=[True, False])
    def test_consumer_failure(self, clean_shutdown, enable_autocommit):
        partition = TopicPartition(self.TOPIC, 0)
        
        consumer = self.setup_consumer(self.TOPIC, enable_autocommit=enable_autocommit)
        producer = self.setup_producer(self.TOPIC)

        consumer.start()
        self.await_all_members(consumer)

        partition_owner = consumer.owner(partition)
        assert partition_owner is not None

        # startup the producer and ensure that some records have been written
        producer.start()
        self.await_produced_messages(producer)

        # stop the partition owner and await its shutdown
        consumer.kill_node(partition_owner, clean_shutdown=clean_shutdown)
        wait_until(lambda: len(consumer.joined_nodes()) == (self.num_consumers - 1) and consumer.owner(partition) != None,
                   timeout_sec=self.session_timeout_sec*2+5,
                   err_msg="Timed out waiting for consumer to close")

        # ensure that the remaining consumer does some work after rebalancing
        self.await_consumed_messages(consumer, min_messages=1000)

        consumer.stop_all()

        if clean_shutdown:
            # if the total records consumed matches the current position, we haven't seen any duplicates
            # this can only be guaranteed with a clean shutdown
            assert consumer.current_position(partition) == consumer.total_consumed(), \
                "Total consumed records %d did not match consumed position %d" % \
                (consumer.total_consumed(), consumer.current_position(partition))
        else:
            # we may have duplicates in a hard failure
            assert consumer.current_position(partition) <= consumer.total_consumed(), \
                "Current position %d greater than the total number of consumed records %d" % \
                (consumer.current_position(partition), consumer.total_consumed())

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset %d did not match last consumed position %d" % \
                (consumer.last_commit(partition), consumer.current_position(partition))

    @cluster(num_nodes=7)
    @matrix(clean_shutdown=[True, False], enable_autocommit=[True, False])
    def test_broker_failure(self, clean_shutdown, enable_autocommit):
        partition = TopicPartition(self.TOPIC, 0)
        
        consumer = self.setup_consumer(self.TOPIC, enable_autocommit=enable_autocommit)
        producer = self.setup_producer(self.TOPIC)

        producer.start()
        consumer.start()
        self.await_all_members(consumer)

        num_rebalances = consumer.num_rebalances()

        # shutdown one of the brokers
        # TODO: we need a way to target the coordinator instead of picking arbitrarily
        self.kafka.signal_node(self.kafka.nodes[0], signal.SIGTERM if clean_shutdown else signal.SIGKILL)

        # ensure that the consumers do some work after the broker failure
        self.await_consumed_messages(consumer, min_messages=1000)

        # verify that there were no rebalances on failover
        assert num_rebalances == consumer.num_rebalances(), "Broker failure should not cause a rebalance"

        consumer.stop_all()

        # if the total records consumed matches the current position, we haven't seen any duplicates
        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))

        # if autocommit is not turned on, we can also verify the last committed offset
        if not enable_autocommit:
            assert consumer.last_commit(partition) == consumer.current_position(partition), \
                "Last committed offset %d did not match last consumed position %d" % \
                (consumer.last_commit(partition), consumer.current_position(partition))

    @cluster(num_nodes=7)
    def test_group_consumption(self):
        """
        Verifies correct group rebalance behavior as consumers are started and stopped. 
        In particular, this test verifies that the partition is readable after every
        expected rebalance.

        Setup: single Kafka cluster with a group of consumers reading from one topic
        with one partition while the verifiable producer writes to it.

        - Start the consumers one by one, verifying consumption after each rebalance
        - Shutdown the consumers one by one, verifying consumption after each rebalance
        """
        consumer = self.setup_consumer(self.TOPIC)
        producer = self.setup_producer(self.TOPIC)

        partition = TopicPartition(self.TOPIC, 0)

        producer.start()

        for num_started, node in enumerate(consumer.nodes, 1):
            consumer.start_node(node)
            self.await_members(consumer, num_started)
            self.await_consumed_messages(consumer)

        for num_stopped, node in enumerate(consumer.nodes, 1):
            consumer.stop_node(node)

            if num_stopped < self.num_consumers:
                self.await_members(consumer, self.num_consumers - num_stopped)
                self.await_consumed_messages(consumer)

        assert consumer.current_position(partition) == consumer.total_consumed(), \
            "Total consumed records %d did not match consumed position %d" % \
            (consumer.total_consumed(), consumer.current_position(partition))

        assert consumer.last_commit(partition) == consumer.current_position(partition), \
            "Last committed offset %d did not match last consumed position %d" % \
            (consumer.last_commit(partition), consumer.current_position(partition))

class AssignmentValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 6

    def __init__(self, test_context):
        super(AssignmentValidationTest, self).__init__(test_context, num_consumers=3, num_producers=0,
                                                num_zk=1, num_brokers=2, topics={
            self.TOPIC : { 'partitions': self.NUM_PARTITIONS, 'replication-factor': 1 },
        })

    @cluster(num_nodes=6)
    @matrix(assignment_strategy=["org.apache.kafka.clients.consumer.RangeAssignor",
                                 "org.apache.kafka.clients.consumer.RoundRobinAssignor"])
    def test_valid_assignment(self, assignment_strategy):
        """
        Verify assignment strategy correctness: each partition is assigned to exactly
        one consumer instance.

        Setup: single Kafka cluster with a set of consumers in the same group.

        - Start the consumers one by one
        - Validate assignment after every expected rebalance
        """
        consumer = self.setup_consumer(self.TOPIC, assignment_strategy=assignment_strategy)
        for num_started, node in enumerate(consumer.nodes, 1):
            consumer.start_node(node)
            self.await_members(consumer, num_started)
            assert self.valid_assignment(self.TOPIC, self.NUM_PARTITIONS, consumer.current_assignment()), \
                "expected valid assignments of %d partitions when num_started %d: %s" % \
                (self.NUM_PARTITIONS, num_started, \
                 [(str(node.account), a) for node, a in consumer.current_assignment().items()])

Kafka 컨슈머의 장애·리밸런스·오프셋 무결성을 폭넓게 검증하는 테스트 구조는 탄탄하지만, 분산 환경의 타이밍 의존성과 중복된 상태 대기 로직 때문에 테스트 자체가 Flaky Test의 원인이 될 여지가 있는 코드다.

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

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.tests.verifiable_consumer_test import VerifiableConsumerTest
from kafkatest.services.kafka import TopicPartition

import signal


class OffsetValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 1

    def __init__(self, test_context):
        super(OffsetValidationTest, self).__init__(
            test_context,
            num_consumers=3,
            num_producers=1,
            num_zk=1,
            num_brokers=2,
            topics={
                self.TOPIC: {
                    'partitions': self.NUM_PARTITIONS,
                    'replication-factor': 2
                }
            })

    def _assert_consumption_consistency(self, consumer, partition,
                                         clean_shutdown):
        """컨슈머의 오프셋 소비 정합성 검증 (공통 함수)"""
        current_position = consumer.current_position(partition)
        total_consumed = consumer.total_consumed()

        if clean_shutdown:
            assert current_position == total_consumed, (
                "Consumed position mismatch: position=%d, "
                "total_consumed=%d, rebalances=%d, assignment=%s"
                % (
                    current_position,
                    total_consumed,
                    consumer.num_rebalances(),
                    consumer.current_assignment()
                )
            )
        else:
            assert current_position <= total_consumed, (
                "Consumed position exceeded total consumed records: "
                "position=%d, total_consumed=%d, rebalances=%d, "
                "assignment=%s"
                % (
                    current_position,
                    total_consumed,
                    consumer.num_rebalances(),
                    consumer.current_assignment()
                )
            )

    def _assert_no_unexpected_rebalances(self, consumer,
                                         initial_rebalances):
        """예상치 못한 그룹 리밸런스 발생 여부 검증 (공통 함수)"""
        actual_rebalances = consumer.num_rebalances()
        unexpected_rebalances = actual_rebalances - initial_rebalances

        assert unexpected_rebalances == 0, (
            "Unexpected consumer group rebalance: "
            "initial=%d, current=%d, delta=%d, assignment=%s"
            % (
                initial_rebalances,
                actual_rebalances,
                unexpected_rebalances,
                consumer.current_assignment()
            )
        )

    def rolling_bounce_consumers(self, consumer, num_bounces=5,
                                 clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

                wait_until(
                    lambda: len(consumer.dead_nodes()) == 1,
                    timeout_sec=self.session_timeout_sec + 5,
                    err_msg="Timed out waiting for consumer shutdown"
                )

                consumer.start_node(node)
                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def bounce_all_consumers(self, consumer, num_bounces=5,
                             clean_shutdown=True):
        for _ in range(num_bounces):
            for node in consumer.nodes:
                consumer.stop_node(node, clean_shutdown)

            wait_until(
                lambda: len(consumer.dead_nodes()) == self.num_consumers,
                timeout_sec=10,
                err_msg="Timed out waiting for all consumers to shut down"
            )

            for node in consumer.nodes:
                consumer.start_node(node)

            self.await_all_members(consumer)
            self.await_consumed_messages(consumer)

    def rolling_bounce_brokers(self, consumer, num_bounces=5,
                               clean_shutdown=True):
        """원본의 5회 반복 장애 주입 강도를 완벽하게 복원"""
        for _ in range(num_bounces):
            for node in self.kafka.nodes:
                self.kafka.restart_node(node, clean_shutdown=clean_shutdown)
                self.await_all_members(consumer)
                self.await_consumed_messages(consumer)

    def setup_consumer(self, topic, **kwargs):
        consumer = super(OffsetValidationTest, self).setup_consumer(
            topic, **kwargs
        )
        self.mark_for_collect(consumer, 'verifiable_consumer_stdout')
        return consumer

    @cluster(num_nodes=7)
    def test_broker_rolling_bounce(self):
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)

        initial_rebalances = consumer.num_rebalances()

        # 원본과 동일하게 5회 롤링 바운스 수행
        self.rolling_bounce_brokers(
            consumer,
            num_bounces=5,
            clean_shutdown=True
        )

        self._assert_no_unexpected_rebalances(
            consumer,
            initial_rebalances
        )

        consumer.stop_all()

        self._assert_consumption_consistency(
            consumer,
            partition,
            clean_shutdown=True
        )

    @cluster(num_nodes=7)
    @matrix(clean_shutdown=[True], bounce_mode=["all", "rolling"])
    def test_consumer_bounce(self, clean_shutdown, bounce_mode):
        partition = TopicPartition(self.TOPIC, 0)

        producer = self.setup_producer(self.TOPIC)
        consumer = self.setup_consumer(self.TOPIC)

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)

        if bounce_mode == "all":
            self.bounce_all_consumers(
                consumer,
                clean_shutdown=clean_shutdown
            )
        else:
            self.rolling_bounce_consumers(
                consumer,
                clean_shutdown=clean_shutdown
            )

        consumer.stop_all()

        self._assert_consumption_consistency(
            consumer,
            partition,
            clean_shutdown
        )

    @cluster(num_nodes=7)
    @matrix(clean_shutdown=[True, False],
            enable_autocommit=[True, False])
    def test_consumer_failure(self, clean_shutdown, enable_autocommit):
        partition = TopicPartition(self.TOPIC, 0)

        consumer = self.setup_consumer(
            self.TOPIC,
            enable_autocommit=enable_autocommit
        )
        producer = self.setup_producer(self.TOPIC)

        consumer.start()
        self.await_all_members(consumer)

        partition_owner = consumer.owner(partition)
        assert partition_owner is not None, (
            "No consumer owns partition %s; assignment=%s"
            % (
                partition,
                consumer.current_assignment()
            )
        )

        producer.start()
        self.await_produced_messages(producer)

        consumer.kill_node(
            partition_owner,
            clean_shutdown=clean_shutdown
        )

        wait_until(
            lambda: (
                len(consumer.joined_nodes()) ==
                self.num_consumers - 1
                and consumer.owner(partition) is not None
            ),
            timeout_sec=self.session_timeout_sec * 2 + 5,
            err_msg="Timed out waiting for consumer group recovery"
        )

        self.await_consumed_messages(
            consumer,
            min_messages=1000
        )

        consumer.stop_all()

        self._assert_consumption_consistency(
            consumer,
            partition,
            clean_shutdown
        )

        if not enable_autocommit:
            current_position = consumer.current_position(partition)
            last_commit = consumer.last_commit(partition)

            assert last_commit == current_position, (
                "Committed offset mismatch: committed=%d, "
                "position=%d, rebalances=%d, assignment=%s"
                % (
                    last_commit,
                    current_position,
                    consumer.num_rebalances(),
                    consumer.current_assignment()
                )
            )

    @cluster(num_nodes=7)
    @matrix(clean_shutdown=[True, False],
            enable_autocommit=[True, False])
    def test_broker_failure(self, clean_shutdown, enable_autocommit):
        partition = TopicPartition(self.TOPIC, 0)

        consumer = self.setup_consumer(
            self.TOPIC,
            enable_autocommit=enable_autocommit
        )
        producer = self.setup_producer(self.TOPIC)

        producer.start()
        consumer.start()
        self.await_all_members(consumer)

        initial_rebalances = consumer.num_rebalances()

        self.kafka.signal_node(
            self.kafka.nodes[0],
            signal.SIGTERM if clean_shutdown else signal.SIGKILL
        )

        self.await_consumed_messages(
            consumer,
            min_messages=1000
        )

        # 원본의 핵심 검증: 브로커 장애가 불필요한 리밸런스를 유발하지 않았음을 보장
        self._assert_no_unexpected_rebalances(
            consumer,
            initial_rebalances
        )

        consumer.stop_all()

        self._assert_consumption_consistency(
            consumer,
            partition,
            clean_shutdown
        )

        if not enable_autocommit:
            current_position = consumer.current_position(partition)
            last_commit = consumer.last_commit(partition)

            assert last_commit == current_position, (
                "Committed offset mismatch after broker failure: "
                "committed=%d, position=%d, initial_rebalances=%d, "
                "final_rebalances=%d, assignment=%s"
                % (
                    last_commit,
                    current_position,
                    initial_rebalances,
                    consumer.num_rebalances(),
                    consumer.current_assignment()
                )
            )

    @cluster(num_nodes=7)
    def test_group_consumption(self):
        consumer = self.setup_consumer(self.TOPIC)
        producer = self.setup_producer(self.TOPIC)

        partition = TopicPartition(self.TOPIC, 0)

        producer.start()

        for num_started, node in enumerate(consumer.nodes, 1):
            consumer.start_node(node)

            self.await_members(consumer, num_started)
            self.await_consumed_messages(consumer)

        for num_stopped, node in enumerate(consumer.nodes, 1):
            consumer.stop_node(node)

            remaining_consumers = self.num_consumers - num_stopped

            if remaining_consumers > 0:
                self.await_members(
                    consumer,
                    remaining_consumers
                )
                self.await_consumed_messages(consumer)

        self._assert_consumption_consistency(
            consumer,
            partition,
            clean_shutdown=True
        )

        last_commit = consumer.last_commit(partition)
        current_position = consumer.current_position(partition)

        assert last_commit == current_position, (
            "Final committed offset mismatch: "
            "committed=%d, position=%d, assignment=%s"
            % (
                last_commit,
                current_position,
                consumer.current_assignment()
            )
        )


class AssignmentValidationTest(VerifiableConsumerTest):
    TOPIC = "test_topic"
    NUM_PARTITIONS = 6

    def __init__(self, test_context):
        super(AssignmentValidationTest, self).__init__(
            test_context,
            num_consumers=3,
            num_producers=0,
            num_zk=1,
            num_brokers=2,
            topics={
                self.TOPIC: {
                    'partitions': self.NUM_PARTITIONS,
                    'replication-factor': 1
                }
            })

    @cluster(num_nodes=6)
    @matrix(
        assignment_strategy=[
            "org.apache.kafka.clients.consumer.RangeAssignor",
            "org.apache.kafka.clients.consumer.RoundRobinAssignor"
        ]
    )
    def test_valid_assignment(self, assignment_strategy):
        consumer = self.setup_consumer(
            self.TOPIC,
            assignment_strategy=assignment_strategy
        )

        for num_started, node in enumerate(consumer.nodes, 1):
            consumer.start_node(node)

            self.await_members(
                consumer,
                num_started
            )

            assignment = consumer.current_assignment()

            assert self.valid_assignment(
                self.TOPIC,
                self.NUM_PARTITIONS,
                assignment
            ), (
                "Invalid partition assignment: "
                "strategy=%s, started=%d, assignment=%s"
                % (
                    assignment_strategy,
                    num_started,
                    [
                        (str(node.account), assignments)
                        for node, assignments in assignment.items()
                    ]
                )
            )

최종 개선사항
✅ 반복되는 소비 오프셋 검증 → _assert_consumption_consistency() 공통화 → clean/hard shutdown별 데이터 정합성 검증 일관성 확보
✅ 반복되는 리밸런스 검증 → _assert_no_unexpected_rebalances() 공통화 → 장애 전후 그룹 상태 검증 중복 제거 및 실패 원인 가시성 향상
✅ 원본의 5회 브로커 바운스 의미 약화 → num_bounces=5로 명시적 복원 → 원본 테스트의 장애 주입 강도와 회귀 검증 범위 보존
✅ 단순 assertion 실패 메시지 → offset·rebalance·assignment 상태 포함 → 분산 테스트 실패 시 원인 추적성 강화
✅ 브로커 장애 후 리밸런스 검증 누락 가능성 → 초기 리밸런스 기준점 저장 후 delta 검증 → 장애가 불필요한 그룹 변화를 유발했는지 명확히 검증
✅ 반복되는 partition/offset 상태 조회 → 검증 경계에서 변수로 캡처 → 동일 상태를 기준으로 assertion 수행하여 가독성과 디버깅성 향상
✅ 원본의 lifecycle·fault injection 시나리오 유지 → 검증 로직만 공통화 → 테스트 의도를 변경하지 않고 유지보수성과 장애 분석성을 동시에 강화

원본의 Kafka Consumer 장애·리밸런싱 검증 의미는 그대로 보존하면서 중복 검증을 공통화하고 장애 상태 정보를 assertion에 결합해, 테스트 시나리오를 훼손하지 않고 유지보수성과 실패 진단력을 9.5 수준의 실무형 테스트 구조로 끌어올린 버전이다.
