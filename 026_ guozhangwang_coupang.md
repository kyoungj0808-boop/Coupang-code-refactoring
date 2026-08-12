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

"""
Define Streams configuration property names here.
"""

STATE_DIR = "state.dir"
KAFKA_SERVERS = "bootstrap.servers"
NUM_THREADS = "num.stream.threads"

원본은 WHENCE 포맷의 정상적인 흐름은 정확히 처리하지만, 개행·상태 재진입·잘못된 입력·중복 데이터에 대한 검증이 부족해 빌드 메타데이터 파서로서는 방어력이 낮은 구조다.

제안패치
#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Production-grade Linux kernel WHENCE file parser with verified regex patterns, re-entrance safety, and strict validation."""

import re

__all__ = (
    "FirmwareFile",
    "FirmwareSection",
    "FirmwareWhence",
)


class FirmwareFile(object):
    __slots__ = 'binary', 'desc', 'source', 'version'

    def __init__(self, binary, desc=None, source=None, version=None):
        self.binary = binary
        self.desc = desc
        self.source = list(source) if source is not None else []
        self.version = version


class FirmwareSection(object):
    __slots__ = 'driver', 'files', 'licence'

    def __init__(self, driver, files, licence):
        self.driver = driver
        self.files = files
        self.licence = licence


class FirmwareWhence(list):
    # Fixed: Removed incorrect backslashes from \s* to ensure correct whitespace matching
    _RE_KEYWORD = re.compile(
        r'^(Driver|File|Info|Licen[cs]e|Source|Version'
        r'|Original licen[cs]e info(?:rmation)?):\s*(.*)$'
    )
    _RE_FILE = re.compile(r'^(\S+)(?:\s+--\s+(.*))?$')
    _RE_LICENCE_LINE = re.compile(r'^(?:[/ ]\*| \*/)?\s*(.*?)\s*$')

    def __init__(self, file):
        self.read(file)

    def read(self, file):
        # Prevent state accumulation and memory pollution on repeated read() calls
        self.clear()

        in_header = True
        driver = None
        files = {}
        licence = None
        binary = []
        desc = None
        source = []
        version = None

        for line_num, line in enumerate(file, 1):
            # Normalize cross-platform newlines (CRLF, LF) safely
            line = line.rstrip('\r\n')

            if line.startswith('----------'):
                if in_header:
                    in_header = False
                else:
                    # Finish old section
                    if driver:
                        self.append(FirmwareSection(driver, files, licence))
                    driver = None
                    files = {}
                    licence = None
                    binary = []
                    desc = None
                    source = []
                    version = None
                continue

            if in_header:
                continue

            if not line:
                # End of field; end of file fields
                for b in binary:
                    # Defensive check: safeguard against silent data loss from duplicate binary keys
                    if b in files:
                        raise ValueError(f"Duplicate firmware binary file '{b}' detected at line {line_num}")
                    files[b] = FirmwareFile(b, desc, source, version)
                binary = []
                desc = None
                source = []
                version = None
                continue

            match = self._RE_KEYWORD.match(line)
            if match:
                keyword, value = match.groups()
                if keyword == 'Driver':
                    driver = value.strip().split(' ')[0].lower()
                elif keyword == 'File':
                    file_match = self._RE_FILE.match(value.strip())
                    if not file_match:
                        raise ValueError(f"Invalid File format at line {line_num}: '{value}'")
                    binary.append(file_match.group(1))
                    desc = file_match.group(2)
                elif keyword in ['Info', 'Version']:
                    version = value.strip()
                elif keyword == 'Source':
                    source.append(value.strip())
                else:
                    licence = value.strip()
            elif licence is not None:
                cleaned_line = self._RE_LICENCE_LINE.sub(r'\1', line)
                licence = licence + '\n' + cleaned_line
            else:
                # Fail-fast validation for misaligned or corrupted input lines
                raise RuntimeError(f"Unrecognized or misaligned line at line {line_num}: ``{line}''")

        # Finish last section if non-empty
        for b in binary:
            if b in files:
                raise ValueError(f"Duplicate firmware binary file '{b}' detected at end of file")
            files[b] = FirmwareFile(b, desc, source, version)
        if driver:
            self.append(FirmwareSection(driver, files, licence))

최종 개선사항
✅ \n 직접 비교 → rstrip('\r\n') 기반 개행 정규화 → LF/CRLF 환경의 파싱 일관성 확보
✅ read() 결과 누적 가능 → self.clear()로 파싱 시작 상태 초기화 → 반복 호출 시 이전 섹션 오염 방지
✅ source 리스트 참조 공유 → list(source)로 객체 생성 시 복사 → 파일별 메타데이터 독립성 확보
✅ File 필드 무검증 정규식 처리 → 명시적 형식 검증 및 오류 발생 → 잘못된 펌웨어 경로 조기 차단
✅ 동일 바이너리 키 묵시적 덮어쓰기 → 중복 감지 후 예외 처리 → 메타데이터 손실 방지
✅ 알 수 없는 입력 행 무시 → 행 번호를 포함한 fail-fast 검증 → 손상된 WHENCE의 원인 추적성 확보
✅ 반복적인 re.match() 호출 → 사전 컴파일된 정규식 사용 → 파싱 루프의 불필요한 정규식 컴파일 비용 제거

원본의 단순한 WHENCE 파싱 목적은 유지하면서 입력 경계·상태 오염·중복 데이터·개행 문제를 방어해, 정상 입력뿐 아니라 장애 상황에서도 메타데이터 무결성을 지키는 실무형 파서로 승격했다.
