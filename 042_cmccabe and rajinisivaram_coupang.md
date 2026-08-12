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

from kafkatest.services.trogdor.task_spec import TaskSpec


class ProcessStopFaultSpec(TaskSpec):
    """
    The specification for a process stop fault.
    """

    def __init__(self, start_ms, duration_ms, nodes, java_process_name):
        """
        Create a new ProcessStopFaultSpec.

        :param start_ms:            The start time, as described in task_spec.py
        :param duration_ms:         The duration in milliseconds.
        :param node_names:          An array describing the nodes to stop processes on.  The array
                                    may contain either node names, or ClusterNode objects.
        :param java_process_name:   The name of the java process to stop.  This is the name which
                                    is reported by jps, etc., not the OS-level process name.
        """
        super(ProcessStopFaultSpec, self).__init__(start_ms, duration_ms)
        self.message["class"] = "org.apache.kafka.trogdor.fault.ProcessStopFaultSpec"
        self.message["nodeNames"] = TaskSpec.to_node_names(nodes)
        self.message["javaProcessName"] = java_process_name

원본은 Trogdor의 TaskSpec 계약을 정확히 따르는 단순·명확한 장애 주입 스펙이지만, 입력 계약을 부모 클래스에 전적으로 의존하고 문서의 node_names 불일치까지 남아 있어, 작은 보강으로 더 견고해질 여지가 있는 9점 안팎의 코드다.

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

from kafkatest.services.trogdor.task_spec import TaskSpec


class ProcessStopFaultSpec(TaskSpec):
    """
    The specification for a process stop fault.
    (Enterprise Strict Contract Validation Version)
    """

    def __init__(self, start_ms, duration_ms, nodes, java_process_name):
        """
        Create a new ProcessStopFaultSpec with strict millisecond integer validation.

        :param start_ms:            The start time in milliseconds (strictly int, excluding bool).
        :param duration_ms:         The duration in milliseconds (strictly int, excluding bool).
        :param nodes:               An array describing the nodes to stop processes on.
        :param java_process_name:   The name of the java process to stop.
        """
        # Python에서 isinstance(True, int)는 True이므로 bool 타입을 명시적으로 배제하고 정수형(int)만 허용
        if isinstance(start_ms, bool) or not isinstance(start_ms, int):
            raise TypeError("start_ms must be an integer representing milliseconds.")
        if isinstance(duration_ms, bool) or not isinstance(duration_ms, int):
            raise TypeError("duration_ms must be an integer representing milliseconds.")
        if not isinstance(java_process_name, str) or not java_process_name.strip():
            raise ValueError("java_process_name must be a non-empty string.")

        super(ProcessStopFaultSpec, self).__init__(start_ms, duration_ms)
        self.message["class"] = "org.apache.kafka.trogdor.fault.ProcessStopFaultSpec"
        self.message["nodeNames"] = TaskSpec.to_node_names(nodes)
        self.message["javaProcessName"] = java_process_name

최종 개선사항
✅ 시간 인자 무검증 → bool을 배제한 정수형 밀리초 계약 검증 → 잘못된 장애 스케줄의 조기 차단
✅ 프로세스명 입력 무검증 → 공백을 제외한 비어 있지 않은 문자열 검증 → 장애 대상 식별자 무결성 확보
✅ 잘못된 입력이 직렬화·Trogdor 실행 단계까지 전달 → 생성 시점에서 명확한 TypeError·ValueError 발생 → 장애 원인 추적성과 실패 격리 강화
✅ nodes 변환을 자체적으로 중복 검증 → 기존 TaskSpec.to_node_names() 계약 활용 → 프레임워크 호환성 유지와 불필요한 과설계 방지
✅ Trogdor Java 클래스 식별자를 임의 추상화 → 기존 프로토콜 식별자 그대로 유지 → Python 테스트 계층과 Trogdor 실행 계층의 계약 보존
✅ 단순 Spec 객체에 복잡한 예외·로깅 계층 추가 → 생성자 수준의 최소 방어 검증으로 제한 → 코드 단순성과 유지보수성 동시 확보

원본의 단순한 Trogdor Spec 구조는 유지하면서 입력 계약과 장애 스케줄 무결성을 강화해, 불필요한 과설계 없이 실무형 방어 구조로 승격되었다.
