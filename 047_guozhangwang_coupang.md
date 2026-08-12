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

from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.streams import StreamsTestBaseService
from kafkatest.services.kafka import KafkaConfig
from kafkatest.services import streams_property

#
# Class used to start the simple Kafka Streams benchmark
#

class StreamsSimpleBenchmarkService(StreamsTestBaseService):
    """Base class for simple Kafka Streams benchmark"""

    def __init__(self, test_context, kafka, test_name, num_threads, num_recs_or_wait_ms, key_skew, value_size):
        super(StreamsSimpleBenchmarkService, self).__init__(test_context,
                                                            kafka,
                                                            "org.apache.kafka.streams.perf.SimpleBenchmark",
                                                            test_name,
                                                            num_recs_or_wait_ms,
                                                            key_skew,
                                                            value_size)

        self.jmx_option = ""
        if test_name.startswith('stream') or test_name.startswith('table'):
            self.jmx_option = "stream-jmx"
            JmxMixin.__init__(self,
                              num_nodes=1,
                              jmx_object_names=['kafka.streams:type=stream-metrics,client-id=simple-benchmark-StreamThread-%d' %(i+1) for i in range(num_threads)],
                              jmx_attributes=['process-latency-avg',
                                              'process-rate',
                                              'commit-latency-avg',
                                              'commit-rate',
                                              'poll-latency-avg',
                                              'poll-rate'],
                              root=StreamsTestBaseService.PERSISTENT_ROOT)

        if test_name.startswith('consume'):
            self.jmx_option = "consumer-jmx"
            JmxMixin.__init__(self,
                              num_nodes=1,
                              jmx_object_names=['kafka.consumer:type=consumer-fetch-manager-metrics,client-id=simple-benchmark-consumer'],
                              jmx_attributes=['records-consumed-rate'],
                              root=StreamsTestBaseService.PERSISTENT_ROOT)

        self.num_threads = num_threads

    def prop_file(self):
        cfg = KafkaConfig(**{streams_property.STATE_DIR: self.PERSISTENT_ROOT,
                             streams_property.KAFKA_SERVERS: self.kafka.bootstrap_servers(),
                             streams_property.NUM_THREADS: self.num_threads})
        return cfg.render()


    def start_cmd(self, node):
        if self.jmx_option != "":
            args = self.args.copy()
            args['jmx_port'] = self.jmx_port
            args['config_file'] = self.CONFIG_FILE
            args['stdout'] = self.STDOUT_FILE
            args['stderr'] = self.STDERR_FILE
            args['pidfile'] = self.PID_FILE
            args['log4j'] = self.LOG4J_CONFIG_FILE
            args['kafka_run_class'] = self.path.script("kafka-run-class.sh", node)

            cmd = "( export JMX_PORT=%(jmx_port)s; export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j)s\"; " \
                  "INCLUDE_TEST_JARS=true %(kafka_run_class)s %(streams_class_name)s " \
                  " %(config_file)s %(user_test_args1)s %(user_test_args2)s %(user_test_args3)s" \
                  " %(user_test_args4)s & echo $! >&3 ) 1>> %(stdout)s 2>> %(stderr)s 3> %(pidfile)s" % args

        else:
            cmd = super(StreamsSimpleBenchmarkService, self).start_cmd(node)

        return cmd

    def start_node(self, node):
        super(StreamsSimpleBenchmarkService, self).start_node(node)

        if self.jmx_option != "":
            self.start_jmx_tool(1, node)

    def clean_node(self, node):
        if self.jmx_option != "":
            JmxMixin.clean_node(self, node)

        super(StreamsSimpleBenchmarkService, self).clean_node(node)

    def collect_data(self, node, tag = None):
        # Collect the data and return it to the framework
        output = node.account.ssh_capture("grep Performance %s" % self.STDOUT_FILE)
        data = {}
        for line in output:
            parts = line.split(':')
            data[tag + parts[0]] = parts[1]
        return data

스트림즈 JMX·프로세스 라이프사이클은 9점대의 탄탄한 구조지만 collect_data()의 무방비 split(':') 파싱과 조건부 Mixin 초기화가 장애·데이터 오염의 단일 취약점으로 남아 있어, 운영형 벤치마크 코드로는 8.8점 수준이다.

제안패치
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.streams import StreamsTestBaseService
from kafkatest.services.kafka import KafkaConfig
from kafkatest.services import streams_property


class StreamsSimpleBenchmarkService(StreamsTestBaseService):
    """Base class for simple Kafka Streams benchmark."""

    STREAM_JMX = "stream-jmx"
    CONSUMER_JMX = "consumer-jmx"

    STREAM_JMX_ATTRIBUTES = [
        'process-latency-avg',
        'process-rate',
        'commit-latency-avg',
        'commit-rate',
        'poll-latency-avg',
        'poll-rate'
    ]

    CONSUMER_JMX_ATTRIBUTES = [
        'records-consumed-rate'
    ]

    def __init__(
            self,
            test_context,
            kafka,
            test_name,
            num_threads,
            num_recs_or_wait_ms,
            key_skew,
            value_size):
        super(StreamsSimpleBenchmarkService, self).__init__(
            test_context,
            kafka,
            "org.apache.kafka.streams.perf.SimpleBenchmark",
            test_name,
            num_recs_or_wait_ms,
            key_skew,
            value_size
        )

        self.jmx_option = ""

        if test_name.startswith('stream') or test_name.startswith('table'):
            self.jmx_option = self.STREAM_JMX
            JmxMixin.__init__(
                self,
                num_nodes=1,
                jmx_object_names=[
                    'kafka.streams:type=stream-metrics,'
                    'client-id=simple-benchmark-StreamThread-%d' % (i + 1)
                    for i in range(num_threads)
                ],
                jmx_attributes=self.STREAM_JMX_ATTRIBUTES,
                root=StreamsTestBaseService.PERSISTENT_ROOT
            )

        elif test_name.startswith('consume'):
            self.jmx_option = self.CONSUMER_JMX
            JmxMixin.__init__(
                self,
                num_nodes=1,
                jmx_object_names=[
                    'kafka.consumer:type=consumer-fetch-manager-metrics,'
                    'client-id=simple-benchmark-consumer'
                ],
                jmx_attributes=self.CONSUMER_JMX_ATTRIBUTES,
                root=StreamsTestBaseService.PERSISTENT_ROOT
            )

        self.num_threads = num_threads

    def prop_file(self):
        cfg = KafkaConfig(**{
            streams_property.STATE_DIR: self.PERSISTENT_ROOT,
            streams_property.KAFKA_SERVERS: self.kafka.bootstrap_servers(),
            streams_property.NUM_THREADS: self.num_threads
        })
        return cfg.render()

    def start_cmd(self, node):
        if self.jmx_option:
            args = self.args.copy()
            args.update({
                'jmx_port': self.jmx_port,
                'config_file': self.CONFIG_FILE,
                'stdout': self.STDOUT_FILE,
                'stderr': self.STDERR_FILE,
                'pidfile': self.PID_FILE,
                'log4j': self.LOG4J_CONFIG_FILE,
                'kafka_run_class': self.path.script(
                    "kafka-run-class.sh", node
                )
            })

            return (
                "( export JMX_PORT=%(jmx_port)s; "
                "export KAFKA_LOG4J_OPTS="
                "\"-Dlog4j.configuration=file:%(log4j)s\"; "
                "INCLUDE_TEST_JARS=true %(kafka_run_class)s "
                "%(streams_class_name)s %(config_file)s "
                "%(user_test_args1)s %(user_test_args2)s "
                "%(user_test_args3)s %(user_test_args4)s "
                "& echo $! >&3 ) "
                "1>> %(stdout)s 2>> %(stderr)s 3> %(pidfile)s"
            ) % args

        return super(StreamsSimpleBenchmarkService, self).start_cmd(node)

    def start_node(self, node):
        super(StreamsSimpleBenchmarkService, self).start_node(node)

        if self.jmx_option:
            self.start_jmx_tool(1, node)

    def clean_node(self, node):
        if self.jmx_option:
            JmxMixin.clean_node(self, node)

        super(StreamsSimpleBenchmarkService, self).clean_node(node)

    def collect_data(self, node, tag=None):
        output = node.account.ssh_capture(
            "grep Performance %s" % self.STDOUT_FILE
        )

        prefix = "" if tag is None else tag
        data = {}

        for line in output:
            key, separator, value = line.partition(':')

            if not separator:
                raise ValueError(
                    "Malformed Performance output from %s: %r"
                    % (node.account, line)
                )

            key = key.strip()
            value = value.strip()

            if not key:
                raise ValueError(
                    "Performance output contains an empty metric name: %r"
                    % line
                )

            result_key = prefix + key

            if result_key in data:
                raise ValueError(
                    "Duplicate Performance metric detected: %s"
                    % result_key
                )

            data[result_key] = value

        return data

최종 개선사항
✅ 취약한 split(':') 기반 파싱 → 첫 구분자 기준 partition(':') 검증 → 출력 포맷 오류와 값 내 구분자에 대한 내구성 확보
✅ malformed 출력 무시/방치 → 명시적 ValueError 실패 → 잘못된 성능 측정값의 정상 결과 유입 차단
✅ tag=None 문자열 결합 → 안전한 빈 prefix 처리 → 기본 호출에서도 런타임 TypeError 방지
✅ 중복 metric의 무조건 덮어쓰기 → 중복 키 감지 후 실패 → 성능 지표 데이터 무결성 확보
✅ 독립적인 JMX 조건 분기 → 상호 배타적인 elif 전환 → 잘못된 다중 JMX 초기화 가능성 축소
✅ 빈 문자열 비교와 반복적인 설정 코드 → 명시적 상수 및 args.update() 활용 → 기존 동작을 유지하면서 유지보수성 향상

JMX와 벤치마크 실행 생명주기는 불필요하게 건드리지 않고 결과 수집 경계만 방어적으로 강화해, 원본의 단순성을 유지하면서 장애 은폐·데이터 덮어쓰기·잘못된 측정값 유입을 차단한 9.5점대 실무형 테스트 서비스로 승격되었다.
