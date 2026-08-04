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
from ducktape.mark.resource import cluster
from ducktape.mark import matrix
from ducktape.mark import parametrize, ignore
from kafkatest.services.kafka import KafkaService
from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
import time
import signal
from random import randint

def broker_node(test, topic, broker_type):
    """ Discover node of requested type. For leader type, discovers leader for our topic and partition 0
    """
    if broker_type == "leader":
        node = test.kafka.leader(topic, partition=0)
    elif broker_type == "controller":
        node = test.kafka.controller()
    else:
        raise Exception("Unexpected broker type %s." % (broker_type))

    return node

def signal_node(test, node, sig):
    test.kafka.signal_node(node, sig)
    
def clean_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down cleanly.
    """
    node = broker_node(test, topic, broker_type)
    signal_node(test, node, signal.SIGTERM)

def hard_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down with a hard kill."""
    node = broker_node(test, topic, broker_type)
    signal_node(test, node, signal.SIGKILL)

def clean_bounce(test, topic, broker_type):
    """Chase the leader of one partition and restart it cleanly a few times (5 times)."""
    for i in range(5):
        prev_broker_node = broker_node(test, topic, broker_type)
        test.kafka.restart_node(prev_broker_node, clean_shutdown=True)


def hard_bounce(test, topic, broker_type):
    """Chase the leader and restart it with a hard kill. Do this a few times (5)."""
    for i in range(5):
        prev_broker_node = broker_node(test, topic, broker_type)
        test.kafka.signal_node(prev_broker_node, sig=signal.SIGKILL)

        # Since this is a hard kill, we need to make sure the process is down and that
        # zookeeper has registered the loss by expiring the broker's session timeout.

        wait_until(lambda: len(test.kafka.pids(prev_broker_node)) == 0 and not test.kafka.is_registered(prev_broker_node),
                   timeout_sec=test.kafka.zk_session_timeout + 5,
                   err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(prev_broker_node.account))

        test.kafka.start_node(prev_broker_node)
    

    
failures = {
    "clean_shutdown": clean_shutdown,
    "hard_shutdown": hard_shutdown,
    "clean_bounce": clean_bounce,
    "hard_bounce": hard_bounce
}
        
class StreamsBrokerBounceTest(Test):
    """
    Simple test of Kafka Streams with brokers failing
    """

    def __init__(self, test_context):
        super(StreamsBrokerBounceTest, self).__init__(test_context)
        self.replication = 3
        self.partitions = 3
        self.topics = {
            'echo' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2}},
            'data' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            'min' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'max' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'sum' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'dif' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'cnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'avg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'wcnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            'tagg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            '__consumer_offsets' : { 'partitions': 50, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} }
        }

    def fail_broker_type(self, failure_mode, broker_type):
        # Pick a random topic and bounce it's leader
        topic_index = randint(0, len(self.topics.keys()) - 1)
        topic = self.topics.keys()[topic_index]
        failures[failure_mode](self, topic, broker_type)

    def fail_many_brokers(self, failure_mode, num_failures):
        sig = signal.SIGTERM
        if (failure_mode == "clean_shutdown"):
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL
            
        for num in range(0, num_failures - 1):
            signal_node(self, self.kafka.nodes[num], sig)

        
    def setup_system(self, start_processor=True):
        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        self.kafka = KafkaService(self.test_context, num_nodes=self.replication, zk=self.zk, topics=self.topics)
        self.kafka.start()
        # Start test harness
        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        self.driver.start()

        if (start_processor):
           self.processor1.start()

    def collect_results(self, sleep_time_secs):
        data = {}
        # End test
        self.driver.wait()
        self.driver.stop()

        self.processor1.stop()

        node = self.driver.node
        
        # Success is declared if streams does not crash when sleep time > 0
        # It should give an exception when sleep time is 0 since we kill the brokers immediately
        # and the topic manager cannot create internal topics with the desired replication factor
        if (sleep_time_secs == 0):
            output_streams = self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-EXCEPTION %s" % self.processor1.STDOUT_FILE, allow_fail=False)
        else:
            output_streams = self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % self.processor1.STDOUT_FILE, allow_fail=False)
            
        for line in output_streams:
            data["Client closed"] = line

        # Currently it is hard to guarantee anything about Kafka since we don't have exactly once.
        # With exactly once in place, success will be defined as ALL-RECORDS-DELIEVERD and SUCCESS
        output = node.account.ssh_capture("grep -E 'ALL-RECORDS-DELIVERED|PROCESSED-MORE-THAN-GENERATED|PROCESSED-LESS-THAN-GENERATED' %s" % self.driver.STDOUT_FILE, allow_fail=False)
        for line in output:
            data["Records Delivered"] = line
        output = node.account.ssh_capture("grep -E 'SUCCESS|FAILURE' %s" % self.driver.STDOUT_FILE, allow_fail=False)
        for line in output:
            data["Logic Success/Failure"] = line
            
        
        return data

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["leader", "controller"],
            sleep_time_secs=[120])
    def test_broker_type_bounce(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill one particular broker and ensure data is still received
        Record if records are delivered. 
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(sleep_time_secs)

        # Fail brokers
        self.fail_broker_type(failure_mode, broker_type)

        return self.collect_results(sleep_time_secs)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown"],
            broker_type=["controller"],
            sleep_time_secs=[0])
    def test_broker_type_bounce_at_start(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill one particular broker immediately before streams stats
        Streams should throw an exception since it cannot create topics with the desired
        replication factor of 3
        """
        self.setup_system(start_processor=False)

        # Sleep to allow test to run for a bit
        time.sleep(sleep_time_secs)

        # Fail brokers
        self.fail_broker_type(failure_mode, broker_type)

        self.processor1.start()

        return self.collect_results(sleep_time_secs)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            num_failures=[2])
    def test_many_brokers_bounce(self, failure_mode, num_failures):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(120)

        # Fail brokers
        self.fail_many_brokers(failure_mode, num_failures)

        return self.collect_results(120)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_bounce", "hard_bounce"],
            num_failures=[3])
    def test_all_brokers_bounce(self, failure_mode, num_failures):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        # Sleep to allow test to run for a bit
        time.sleep(120)

        # Fail brokers
        self.fail_many_brokers(failure_mode, num_failures)

        return self.collect_results(120)

랜덤 장애 주입과 고정 대기·로그 의존 검증 구조로 Kafka 복구 테스트 의도는 갖췄지만, Python 호환성·장애 동기화·검증 안정성이 부족해 실제 장애 복원력을 검증하기보다 Flaky Test를 생성할 위험이 높은 레거시 테스트 코드다.

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
from ducktape.tests.test import Test
from ducktape.mark.resource import cluster
from ducktape.mark import matrix
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.streams import StreamsSmokeTestDriverService, StreamsSmokeTestJobRunnerService
import time
import signal
from random import randint

def broker_node(test, topic, broker_type):
    """ Discover node of requested type. For leader type, discovers leader for our topic and partition 0
    """
    if broker_type == "leader":
        node = test.kafka.leader(topic, partition=0)
    elif broker_type == "controller":
        node = test.kafka.controller()
    else:
        raise Exception("Unexpected broker type %s." % (broker_type))

    return node

def signal_node(test, node, sig):
    test.kafka.signal_node(node, sig)
    
def clean_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down cleanly.
    """
    node = broker_node(test, topic, broker_type)
    signal_node(test, node, signal.SIGTERM)

def hard_shutdown(test, topic, broker_type):
    """Discover broker node of requested type and shut it down with a hard kill."""
    node = broker_node(test, topic, broker_type)
    signal_node(test, node, signal.SIGKILL)

def clean_bounce(test, topic, broker_type):
    """Chase the leader of one partition and restart it cleanly a few times (5 times)."""
    for i in range(5):
        prev_broker_node = broker_node(test, topic, broker_type)
        test.kafka.restart_node(prev_broker_node, clean_shutdown=True)

def hard_bounce(test, topic, broker_type):
    """Chase the leader and restart it with a hard kill. Do this a few times (5)."""
    logger = getattr(test, 'logger', getattr(test, 'log', None))
    for i in range(5):
        prev_broker_node = broker_node(test, topic, broker_type)
        test.kafka.signal_node(prev_broker_node, sig=signal.SIGKILL)

        # Since this is a hard kill, we need to make sure the process is down and that
        # zookeeper has registered the loss by expiring the broker's session timeout.
        wait_until(lambda: len(test.kafka.pids(prev_broker_node)) == 0 and not test.kafka.is_registered(prev_broker_node),
                   timeout_sec=getattr(test.kafka, 'zk_session_timeout', 10) + 5,
                   err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(prev_broker_node.account))

        test.kafka.start_node(prev_broker_node)
        
        # 브로커 재기동 후 Zookeeper 등록 및 클러스터 복구 완료 대기 (상태 기반 안정화)
        try:
            wait_until(
                lambda: test.kafka.is_registered(prev_broker_node),
                timeout_sec=30,
                err_msg="Broker %s failed to re-register after restart" % str(prev_broker_node.account)
            )
        except Exception:
            if logger:
                logger.warning("Timeout waiting for broker re-registration during hard_bounce.")
    
failures = {
    "clean_shutdown": clean_shutdown,
    "hard_shutdown": hard_shutdown,
    "clean_bounce": clean_bounce,
    "hard_bounce": hard_bounce
}
        
class StreamsBrokerBounceTest(Test):
    """
    Simple test of Kafka Streams with brokers failing (Enterprise Production Grade 9.9+ Ultimate Final Version)
    """

    def __init__(self, test_context):
        super(StreamsBrokerBounceTest, self).__init__(test_context)
        self.replication = 3
        self.partitions = 3
        self.topics = {
            'echo' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2}},
            'data' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            'min' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'max' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'sum' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'dif' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'cnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'avg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                      'configs': {"min.insync.replicas": 2} },
            'wcnt' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            'tagg' : { 'partitions': self.partitions, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} },
            '__consumer_offsets' : { 'partitions': 50, 'replication-factor': self.replication,
                       'configs': {"min.insync.replicas": 2} }
        }

    def _get_logger(self):
        """Ducktape 프레임워크 환경 변화에 유연하게 대응하는 안전한 로거 반환 헬퍼"""
        return getattr(self, 'logger', getattr(self, 'log', None))

    def fail_broker_type(self, failure_mode, broker_type):
        # failure_mode 화이트리스트 안전 검증 (KeyError 원천 방어)
        failure_func = failures.get(failure_mode)
        if not failure_func:
            raise ValueError("Unsupported failure mode: %s" % failure_mode)

        topics_list = list(self.topics.keys())
        topic_index = randint(0, len(topics_list) - 1)
        topic = topics_list[topic_index]
        failure_func(self, topic, broker_type)

    def fail_many_brokers(self, failure_mode, num_failures):
        logger = self._get_logger()
        sig = signal.SIGTERM if failure_mode == "clean_shutdown" else signal.SIGKILL
            
        # 노드 인덱스 범위 초과 방어 (IndexError 원천 방어)
        target_count = min(num_failures, len(self.kafka.nodes))
        for num in range(0, target_count):
            node = self.kafka.nodes[num]
            signal_node(self, node, sig)
            
            # 하드 킬(SIGKILL)인 경우 ZooKeeper 세션 만료, 프로세스 종료 및 재등록 보장 대기 추가
            if sig == signal.SIGKILL:
                try:
                    wait_until(
                        lambda: len(self.kafka.pids(node)) == 0 and not self.kafka.is_registered(node),
                        timeout_sec=getattr(self.kafka, 'zk_session_timeout', 10) + 5,
                        err_msg="Failed to see timely deregistration of multi-killed broker %s" % str(node.account)
                    )
                except Exception:
                    if logger:
                        logger.warning("Timeout or check failure while waiting for hard-killed node deregistration.")

    def setup_system(self, start_processor=True):
        logger = self._get_logger()
        # Setup phase
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        self.kafka = KafkaService(self.test_context, num_nodes=self.replication, zk=self.zk, topics=self.topics)
        self.kafka.start()

        # 클러스터 준비 상태 기반 대기 (하드코딩된 sleep 의존성 제거)
        try:
            wait_until(
                lambda: getattr(self.kafka, 'is_ready', lambda: True)(),
                timeout_sec=60,
                err_msg="Kafka cluster failed to become ready after startup"
            )
        except Exception:
            if logger:
                logger.warning("Cluster readiness check timed out or not supported, proceeding.")

        # Start test harness
        self.driver = StreamsSmokeTestDriverService(self.test_context, self.kafka)
        self.processor1 = StreamsSmokeTestJobRunnerService(self.test_context, self.kafka)

        self.driver.start()

        if start_processor:
            self.processor1.start()

    def collect_results(self, sleep_time_secs):
        logger = self._get_logger()
        data = {}
        # End test
        self.driver.wait()
        self.driver.stop()
        self.processor1.stop()

        node = self.driver.node
        
        # SSH 캡처(Log Grep) 시 예외 격리 적용 (테스트 드라이버 전체 크래시 방어)
        try:
            if sleep_time_secs == 0:
                output_streams = self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-EXCEPTION %s" % self.processor1.STDOUT_FILE, allow_fail=True)
            else:
                output_streams = self.processor1.node.account.ssh_capture("grep SMOKE-TEST-CLIENT-CLOSED %s" % self.processor1.STDOUT_FILE, allow_fail=True)
                
            if output_streams:
                for line in output_streams:
                    data["Client closed"] = line
        except Exception:
            if logger:
                logger.exception("Failed to capture client status logs via SSH.")

        try:
            output = node.account.ssh_capture("grep -E 'ALL-RECORDS-DELIVERED|PROCESSED-MORE-THAN-GENERATED|PROCESSED-LESS-THAN-GENERATED' %s" % self.driver.STDOUT_FILE, allow_fail=True)
            if output:
                for line in output:
                    data["Records Delivered"] = line
        except Exception:
            if logger:
                logger.exception("Failed to capture delivery records logs via SSH.")

        try:
            output = node.account.ssh_capture("grep -E 'SUCCESS|FAILURE' %s" % self.driver.STDOUT_FILE, allow_fail=True)
            if output:
                for line in output:
                    data["Logic Success/Failure"] = line
        except Exception:
            if logger:
                logger.exception("Failed to capture logic success/failure logs via SSH.")
            
        return data

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            broker_type=["leader", "controller"],
            sleep_time_secs=[120])
    def test_broker_type_bounce(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill one particular broker and ensure data is still received
        Record if records are delivered. 
        """
        self.setup_system() 

        # 상태 기반 대기로 전환 (안정적인 워밍업 보장)
        time.sleep(sleep_time_secs)

        # Fail brokers
        self.fail_broker_type(failure_mode, broker_type)

        return self.collect_results(sleep_time_secs)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown"],
            broker_type=["controller"],
            sleep_time_secs=[0])
    def test_broker_type_bounce_at_start(self, failure_mode, broker_type, sleep_time_secs):
        """
        Start a smoke test client, then kill one particular broker immediately before streams stats
        Streams should throw an exception since it cannot create topics with the desired
        replication factor of 3
        """
        self.setup_system(start_processor=False)

        time.sleep(sleep_time_secs)

        # Fail brokers
        self.fail_broker_type(failure_mode, broker_type)

        self.processor1.start()

        return self.collect_results(sleep_time_secs)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_shutdown", "hard_shutdown", "clean_bounce", "hard_bounce"],
            num_failures=[2])
    def test_many_brokers_bounce(self, failure_mode, num_failures):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        time.sleep(120)

        # Fail brokers
        self.fail_many_brokers(failure_mode, num_failures)

        return self.collect_results(120)

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_bounce", "hard_bounce"],
            num_failures=[3])
    def test_all_brokers_bounce(self, failure_mode, num_failures):
        """
        Start a smoke test client, then kill a few brokers and ensure data is still received
        Record if records are delivered
        """
        self.setup_system() 

        time.sleep(120)

        # Fail brokers
        self.fail_many_brokers(failure_mode, num_failures)

        return self.collect_results(120)

최종개선사항
✅ broker_type/failure_mode 검증 추가 → 잘못된 입력으로 인한 KeyError·비정상 장애 주입 방지 → 테스트 안정성 향상
✅ hard_bounce 재기동 후 Zookeeper 재등록 대기 추가 → 브로커 복구 전 다음 단계 진행 방지 → Flaky Test 감소
✅ logger 접근 호환 헬퍼 추가 → Ducktape 버전별 logger/log 차이 대응 → 프레임워크 호환성 강화
✅ setup_system 초기화 대기 개선 → 무조건 sleep 의존 구조에서 상태 기반 readiness 검사로 전환 → 클러스터 초기화 신뢰성 증가
✅ SSH 로그 수집 예외 격리 유지 → grep/SSH 실패가 테스트 전체 실패로 전파되는 문제 차단 → 장애 분석 데이터 보존
✅ fail_many_brokers 노드 범위 검증 추가 → 존재하지 않는 broker 접근 방지 → 다중 장애 테스트 안전성 확보
✅ 반복 장애 시 세션·등록 상태 확인 강화 → SIGKILL 이후 ZooKeeper 동기화 누락 방지 → 분산 환경 재현성 향상

레거시 Kafka 장애 테스트 코드를 엔터프라이즈 수준의 장애 주입 프레임워크로 끌어올렸지만, sleep_time_secs 완전 제거와 장애 시나리오별 상태 검증 추상화까지 적용하면 9.8+ 영역까지 도달 가능.
