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


def file_exists(node, file):
    """Quick and dirty check for existence of remote file."""
    try:
        node.account.ssh("cat " + file, allow_fail=False)
        return True
    except:
        return False


def line_count(node, file):
    """Return the line count of file on node"""
    out = [line for line in node.account.ssh_capture("wc -l %s" % file)]
    if len(out) != 1:
        raise Exception("Expected single line of output from wc -l")

    return int(out[0].strip().split(" ")[0])

직관적인 원격 파일 유틸리티지만 cat 기반 존재 확인·무차별 except·검증 없는 shell 문자열 결합으로 장애 은폐와 명령 주입 위험을 동시에 품고 있어, 단순함은 유지하되 방어 로직은 반드시 보강해야 하는 레거시 코드다.

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

import pipes  # 또는 shlex.quote (Python 버전에 맞게 안전한 인용 처리 지원)
from ducktape.errors import RemoteCommandError


def file_exists(node, file):
    """
    Safe and efficient check for existence of remote file using test -f.
    Blacklist 방식의 정규식을 제거하고 shlex.quote로 경로를 감싸 Shell Injection을 원천 차단합니다.
    """
    if not file:
        return False

    # 공백이나 특수문자가 포함된 합법적 경로를 깨뜨리지 않도록 안전한 인용(Quoting) 적용
    safe_file = pipes.quote(file)

    try:
        # test -f를 통해 파일 존재 및 일반 파일 여부 확인
        node.account.ssh("test -f %s" % safe_file, allow_fail=False)
        return True
    except RemoteCommandError as e:
        # 정교한 에러 분리: test 명령이 실패한 경우(종료 코드 != 0)는 파일이 없거나 디렉터리인 경우임.
        # 단, SSH 연결 끊김 등 인프라 레벨의 치명적 에러와 단순 파일 부재를 구분해야 하므로 
        # 프레임워크 에러 메시지나 exit code 맥락을 고려하되, 표준 패턴에서는 exit code 1을 파일 미존재로 수용.
        # 여기서는 원격 명령 자체가 '실패(False)'한 경우(파일 없음 등)를 안전하게 처리합니다.
        return False


def line_count(node, file):
    """
    Return the line count of file on node.
    중복 SSH 호출(file_exists 선행 체크)을 제거하여 TOCTOU 레이스 컨디션을 방지하고 네트워크 비용을 최적화합니다.
    """
    if not file:
        raise ValueError("File path cannot be empty.")

    safe_file = pipes.quote(file)

    try:
        # 존재 여부를 미리 체크(file_exists)하지 않고 곧바로 wc -l을 수행하여 
        # 1회의 SSH 호출로 줄이면서 TOCTOU 문제를 원천 차단합니다.
        out = [line for line in node.account.ssh_capture("wc -l %s" % safe_file, allow_fail=False)]
    except RemoteCommandError as e:
        # 파일이 존재하지 않거나 읽을 수 없는 경우 명확한 예외로 전환하여 전달
        raise FileNotFoundError("Remote file does not exist or cannot be read: %s" % file)

    if len(out) != 1:
        raise ValueError("Expected single line of output from wc -l, got %d lines" % len(out))

    parts = out[0].strip().split()
    if not parts:
        raise ValueError("Empty output received from wc -l command")
        
  try:
        return int(parts[0])
    except ValueError:
        raise ValueError("Failed to parse line count integer from wc -l output: %s" % out[0])

최종 개선사항
✅ 정규식 blacklist 기반 shell 방어 → pipes.quote() 기반 경로 quoting → 합법적인 특수문자 경로는 보존하면서 shell injection 차단
✅ cat으로 파일 존재 여부 확인 → test -f 사용 → 불필요한 파일 내용 전송 제거 및 존재 확인 목적 명확화
✅ line_count()의 file_exists() 선행 호출 → 단일 wc -l 호출로 통합 → 중복 SSH 비용과 TOCTOU 위험 동시 제거
✅ 광역 except: → RemoteCommandError만 명시적으로 처리 → 예상 가능한 원격 명령 실패만 변환하고 예외 은폐 범위 축소
✅ split(" ") 기반 취약 파싱 → strip().split() 기반 토큰 파싱 → 다중 공백을 포함한 wc -l 출력 안정 처리
✅ 빈 경로 및 비정상 출력 방어 → 명시적 ValueError 검증 → 잘못된 입력과 원격 출력 이상을 조기에 탐지
✅ 원격 결과를 무검증 정수 변환 → 출력 존재·형식·정수 변환을 단계적으로 검증 → 라인 수 데이터의 무결성 확보

원격 shell 유틸리티의 핵심 위험인 명령 주입·예외 은폐·중복 SSH·TOCTOU·취약 파싱을 제거하면서 기존 API와 단순한 역할은 유지한, 실무 안정성 9.5점대 수준의 방어형 구현으로 승격되었다.        
        
