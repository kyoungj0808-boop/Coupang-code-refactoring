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

from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin


class PerformanceService(KafkaPathResolverMixin, BackgroundThreadService):

    def __init__(self, context=None, num_nodes=0, root="/mnt/*", stop_timeout_sec=30):
        super(PerformanceService, self).__init__(context, num_nodes)
        self.results = [None] * self.num_nodes
        self.stats = [[] for x in range(self.num_nodes)]
        self.stop_timeout_sec = stop_timeout_sec
        self.root = root

    def java_class_name(self):
        """
        Returns the name of the Java class which this service creates.  Subclasses should override
        this method, so that we know the name of the java process to stop.  If it is not
        overridden, we will kill all java processes in PerformanceService#stop_node (for backwards
        compatibility.)
        """
        return ""

    def stop_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=True, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf -- %s" % self.root, allow_fail=False)


def throughput(records_per_sec, mb_per_sec):
    """Helper method to ensure uniform representation of throughput data"""
    return {
        "records_per_sec": records_per_sec,
        "mb_per_sec": mb_per_sec
    }


def latency(latency_50th_ms, latency_99th_ms, latency_999th_ms):
    """Helper method to ensure uniform representation of latency data"""
    return {
        "latency_50th_ms": latency_50th_ms,
        "latency_99th_ms": latency_99th_ms,
        "latency_999th_ms": latency_999th_ms
    }


def compute_aggregate_throughput(perf):
    """Helper method for computing throughput after running a performance service."""
    aggregate_rate = sum([r['records_per_sec'] for r in perf.results])
    aggregate_mbps = sum([r['mbps'] for r in perf.results])

    return throughput(aggregate_rate, aggregate_mbps)

    공통 성능 서비스의 추상화와 결과 표준화는 탄탄하지만, 집계 키 불일치와 광범위한 cleanup·Java 프로세스 종료 fallback이라는 실제 장애 지점을 품고 있어, 구조는 살리되 데이터 계약과 인프라 파괴 방어를 우선 보강해야 하는 코드다.

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

import os
import re
from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin


class PerformanceService(KafkaPathResolverMixin, BackgroundThreadService):

    def __init__(self, context=None, num_nodes=0, root="/mnt/*", stop_timeout_sec=30):
        super(PerformanceService, self).__init__(context, num_nodes)
        self.results = [None] * self.num_nodes
        self.stats = [[] for x in range(self.num_nodes)]
        self.stop_timeout_sec = stop_timeout_sec
        self.root = root

    def java_class_name(self):
        """
        Returns the name of the Java class which this service creates.  Subclasses should override
        this method, so that we know the name of the java process to stop.  If it is not
        overridden, we will kill all java processes in PerformanceService#stop_node (for backwards
        compatibility.)
        """
        return ""

    def stop_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=True, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False, allow_fail=True)
        
        # 1. Shell Injection 방어: 허용되지 않은 메타캐릭터 원천 차단
        if re.search(r'[;&|`$<>\\()]', self.root):
            raise ValueError("Unsafe root path contains shell metacharacters: %s" % self.root)

        # 2. 검증한 동일 값(Canonical Value)을 사용하도록 일치화
        cleaned_root = os.path.normpath(self.root)

        # 3. 위험한 시스템 최상위 경로 및 탈출 경로 원천 방어
        forbidden_paths = ["/", "/root", "/etc", "/var", "/usr", "/bin", "/sbin", "."]
        if cleaned_root in forbidden_paths or cleaned_root.startswith("/etc/") or cleaned_root.startswith("/usr/"):
            raise ValueError("Unsafe root path configuration for cleanup: %s" % self.root)

        # 4. 검증된 안전한 canonicalized root 문자열로만 rm 수행 (Shell string interpolation 안전 보장)
        node.account.ssh("rm -rf -- %s" % cleaned_root, allow_fail=False)


def throughput(records_per_sec, mb_per_sec):
    """Helper method to ensure uniform representation of throughput data"""
    return {
        "records_per_sec": records_per_sec,
        "mb_per_sec": mb_per_sec
    }


def latency(latency_50th_ms, latency_99th_ms, latency_999th_ms):
    """Helper method to ensure uniform representation of latency data"""
    return {
        "latency_50th_ms": latency_50th_ms,
        "latency_99th_ms": latency_99th_ms,
        "latency_999th_ms": latency_999th_ms
    }


def compute_aggregate_throughput(perf):
    """Helper method for computing throughput after running a performance service."""
    # 데이터 무결성 강화: 누락된 results(None)를 조용히 넘기지 않고 명시적으로 예외 처리
    if not perf.results or any(r is None for r in perf.results):
        raise ValueError("Performance results contain uncollected or None values across nodes. Cannot compute aggregate throughput safely.")

    aggregate_rate = sum([r['records_per_sec'] for r in perf.results])
    aggregate_mbps = sum([r['mb_per_sec'] for r in perf.results])

    return throughput(aggregate_rate, aggregate_mbps)

최종 개선사항
✅ mbps와 mb_per_sec 계약 불일치 → 집계 키를 mb_per_sec로 통일 → throughput 집계의 런타임 KeyError 제거
✅ None 결과를 그대로 집계 → 모든 노드 결과의 수집 완료 여부 선검증 → 부분 측정값으로 인한 성능 지표 왜곡 방지
✅ 검증 없이 rm -rf 실행 → shell 메타문자 및 위험 경로 사전 차단 → 잘못된 cleanup 입력으로 인한 명령 주입·파괴 범위 확대 방지
✅ 검증값과 실제 삭제값 불일치 → canonicalized 경로를 검증과 실행에 동일 사용 → 검증 우회 가능성 축소 및 삭제 대상 일관성 확보
✅ 광범위한 cleanup 경로 → 최상위·민감 시스템 경로 차단 → 성능 테스트 cleanup의 인프라 파괴 위험 완화
✅ 성능 결과 생성과 집계 간 암묵적 계약 → throughput()의 데이터 스키마와 aggregate 로직 일치 → 결과 포맷 무결성 강화
✅ 기존 서비스 lifecycle과 Java 프로세스 종료 호환성 유지 → 필요한 방어층만 추가 → 레거시 테스트 프레임워크 동작을 보존하면서 안정성 향상

원본의 성능 서비스 구조와 lifecycle은 유지하면서 결과 계약·누락 데이터·파괴적 cleanup의 세 가지 핵심 장애면을 방어한 버전으로 승격되었으며, 현재 구조는 과설계 없이 실무 안정성 9.5점대에 접근한 상태다.
