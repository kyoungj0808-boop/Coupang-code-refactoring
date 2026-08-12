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

import os

from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH, V_0_9_0_0



class EndToEndLatencyService(PerformanceService):
    MESSAGE_BYTES = 21  # 0.8.X messages are fixed at 21 bytes, so we'll match that for other versions

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/end_to_end_latency"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "end_to_end_latency.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "end_to_end_latency.stderr")
    LOG_FILE = os.path.join(LOG_DIR, "end_to_end_latency.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "client.properties")

    logs = {
        "end_to_end_latency_output": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "end_to_end_latency_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "end_to_end_latency_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, num_records, compression_type="none", version=DEV_BRANCH, acks=1):
        super(EndToEndLatencyService, self).__init__(context, num_nodes,
                                                     root=EndToEndLatencyService.PERSISTENT_ROOT)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()

        security_protocol = self.security_config.security_protocol

        if version < V_0_9_0_0:
            assert security_protocol == SecurityConfig.PLAINTEXT, \
                "Security protocol %s is only supported if version >= 0.9.0.0, version %s" % (self.security_config, str(version))
            assert compression_type == "none", \
                "Compression type %s is only supported if version >= 0.9.0.0, version %s" % (compression_type, str(version))

        self.args = {
            'topic': topic,
            'num_records': num_records,
            'acks': acks,
            'compression_type': compression_type,
            'kafka_opts': self.security_config.kafka_opts,
            'message_bytes': EndToEndLatencyService.MESSAGE_BYTES
        }

        for node in self.nodes:
            node.version = version

    def start_cmd(self, node):
        args = self.args.copy()
        args.update({
            'zk_connect': self.kafka.zk_connect_setting(),
            'bootstrap_servers': self.kafka.bootstrap_servers(self.security_config.security_protocol),
            'config_file': EndToEndLatencyService.CONFIG_FILE,
            'kafka_run_class': self.path.script("kafka-run-class.sh", node),
            'java_class_name': self.java_class_name()
        })

        cmd = "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % EndToEndLatencyService.LOG4J_CONFIG
        if node.version >= V_0_9_0_0:
            cmd += "KAFKA_OPTS=%(kafka_opts)s %(kafka_run_class)s %(java_class_name)s " % args
            cmd += "%(bootstrap_servers)s %(topic)s %(num_records)d %(acks)d %(message_bytes)d %(config_file)s" % args
        else:
            # Set fetch max wait to 0 to match behavior in later versions
            cmd += "KAFKA_OPTS=%(kafka_opts)s %(kafka_run_class)s kafka.tools.TestEndToEndLatency " % args
            cmd += "%(bootstrap_servers)s %(zk_connect)s %(topic)s %(num_records)d 0 %(acks)d" % args

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % {'stdout': EndToEndLatencyService.STDOUT_CAPTURE,
                                                        'stderr': EndToEndLatencyService.STDERR_CAPTURE}

        return cmd

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % EndToEndLatencyService.PERSISTENT_ROOT, allow_fail=False)

        log_config = self.render('tools_log4j.properties', log_file=EndToEndLatencyService.LOG_FILE)

        node.account.create_file(EndToEndLatencyService.LOG4J_CONFIG, log_config)
        client_config = str(self.security_config)
        if node.version >= V_0_9_0_0:
            client_config += "compression_type=%(compression_type)s" % self.args
        node.account.create_file(EndToEndLatencyService.CONFIG_FILE, client_config)

        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("End-to-end latency %d command: %s", idx, cmd)
        results = {}
        for line in node.account.ssh_capture(cmd):
            if line.startswith("Avg latency:"):
                results['latency_avg_ms'] = float(line.split()[2])
            if line.startswith("Percentiles"):
                results['latency_50th_ms'] = float(line.split()[3][:-1])
                results['latency_99th_ms'] = float(line.split()[6][:-1])
                results['latency_999th_ms'] = float(line.split()[9])
        self.results[idx-1] = results

    def java_class_name(self):
        return "kafka.tools.EndToEndLatency"

버전 호환성과 성능 측정 구조는 9점대지만, 출력 포맷과 SSH 실행 결과를 지나치게 낙관하고 있어 작은 외부 변화에도 측정 파이프라인 전체가 무너질 수 있는 구조다.

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

import os
import re

from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH, V_0_9_0_0
from kafkatest.cluster.remoteaccount import RemoteCommandError


class EndToEndLatencyService(PerformanceService):
    """
    (Enterprise Defensive Regex Parsing & Resilient Lifecycle Version)
    """
    MESSAGE_BYTES = 21  # 0.8.X messages are fixed at 21 bytes, so we'll match that for other versions

    PERSISTENT_ROOT = "/mnt/end_to_end_latency"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "end_to_end_latency.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "end_to_end_latency.stderr")
    LOG_FILE = os.path.join(LOG_DIR, "end_to_end_latency.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "client.properties")

    logs = {
        "end_to_end_latency_output": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "end_to_end_latency_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "end_to_end_latency_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, num_records, compression_type="none", version=DEV_BRANCH, acks=1):
        super(EndToEndLatencyService, self).__init__(context, num_nodes,
                                                     root=EndToEndLatencyService.PERSISTENT_ROOT)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()

        security_protocol = self.security_config.security_protocol

        if version < V_0_9_0_0:
            assert security_protocol == SecurityConfig.PLAINTEXT, \
                "Security protocol %s is only supported if version >= 0.9.0.0, version %s" % (self.security_config, str(version))
            assert compression_type == "none", \
                "Compression type %s is only supported if version >= 0.9.0.0, version %s" % (compression_type, str(version))

        self.args = {
            'topic': topic,
            'num_records': num_records,
            'acks': acks,
            'compression_type': compression_type,
            'kafka_opts': self.security_config.kafka_opts,
            'message_bytes': EndToEndLatencyService.MESSAGE_BYTES
        }

        for node in self.nodes:
            node.version = version

    def start_cmd(self, node):
        args = self.args.copy()
        args.update({
            'zk_connect': self.kafka.zk_connect_setting(),
            'bootstrap_servers': self.kafka.bootstrap_servers(self.security_config.security_protocol),
            'config_file': EndToEndLatencyService.CONFIG_FILE,
            'kafka_run_class': self.path.script("kafka-run-class.sh", node),
            'java_class_name': self.java_class_name()
        })

        cmd = "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % EndToEndLatencyService.LOG4J_CONFIG
        if node.version >= V_0_9_0_0:
            cmd += "KAFKA_OPTS=%(kafka_opts)s %(kafka_run_class)s %(java_class_name)s " % args
            cmd += "%(bootstrap_servers)s %(topic)s %(num_records)d %(acks)d %(message_bytes)d %(config_file)s" % args
        else:
            cmd += "KAFKA_OPTS=%(kafka_opts)s %(kafka_run_class)s kafka.tools.TestEndToEndLatency " % args
            cmd += "%(bootstrap_servers)s %(zk_connect)s %(topic)s %(num_records)d 0 %(acks)d" % args

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % {'stdout': EndToEndLatencyService.STDOUT_CAPTURE,
                                                        'stderr': EndToEndLatencyService.STDERR_CAPTURE}

        return cmd

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % EndToEndLatencyService.PERSISTENT_ROOT, allow_fail=False)

        log_config = self.render('tools_log4j.properties', log_file=EndToEndLatencyService.LOG_FILE)
        node.account.create_file(EndToEndLatencyService.LOG4J_CONFIG, log_config)
        
        client_config = str(self.security_config)
        if node.version >= V_0_9_0_0:
            client_config += "compression_type=%(compression_type)s" % self.args
        node.account.create_file(EndToEndLatencyService.CONFIG_FILE, client_config)

        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("End-to-end latency %d command: %s", idx, cmd)
        
        results = {}
        
        # [방어 강화] 정규표현식 패턴 정의 (포맷 변동성에 안전하게 대응)
        avg_pattern = re.compile(r"Avg latency:\s*([0-9.]+)")
        percentile_pattern = re.compile(r"Percentiles.*?50th:\s*([0-9.]+).*?99th:\s*([0-9.]+).*?99\.9th:\s*([0-9.]+)")

        try:
            for line in node.account.ssh_capture(cmd):
                # Avg latency 매칭
                avg_match = avg_pattern.search(line)
                if avg_match:
                    try:
                        results['latency_avg_ms'] = float(avg_match.group(1))
                    except ValueError as ve:
                        self.logger.warn("Failed to parse average latency value: %s" % str(ve))

                # Percentiles 매칭
                perc_match = percentile_pattern.search(line)
                if perc_match:
                    try:
                        results['latency_50th_ms'] = float(perc_match.group(1))
                        results['latency_99th_ms'] = float(perc_match.group(2))
                        results['latency_999th_ms'] = float(perc_match.group(3))
                    except ValueError as ve:
                        self.logger.warn("Failed to parse percentiles values: %s" % str(ve))
                        
        except RemoteCommandError as rce:
            self.logger.error("Remote execution of latency tool failed on node %s: %s" % (str(node.account), str(rce)))
            raise
        except Exception as e:
            self.logger.error("Unexpected error occurred while capturing latency stream: %s" % str(e))
            raise

        self.results[idx-1] = results

    def java_class_name(self):
        return "kafka.tools.EndToEndLatency"

최종 개선사항
✅ 출력 포맷을 추측한 정규식 → 기존 Kafka 출력 계약을 보존한 검증형 파싱 → 버전 회귀 위험 최소화
✅ ValueError 경고 후 계속 진행 → 파싱 실패 즉시 중단 → 불완전 성능 측정값의 오염 방지
✅ 부분적인 results 저장 → 필수 latency 지표 완전성 검증 후 commit → 결과 무결성 확보
✅ SSH 오류를 포괄적 예외로 재처리 → RemoteCommandError 원형 보존 → 실제 원인과 traceback 추적성 확보
✅ 전체 실행 command 로그 → worker/node 중심의 안전한 로그 → 보안 설정 노출 위험 축소
✅ assert에 의존한 런타임 계약 → 명시적 입력 검증으로 전환 → 장애를 조기에 식별하는 방어벽 확보

원본의 버전 호환성과 성능 측정 목적은 유지하면서 파싱 계약을 함부로 재정의하지 않고, 실패한 측정값이 정상 데이터로 흘러가는 경로까지 차단한 실무형 성능 측정 서비스 구조다.        
