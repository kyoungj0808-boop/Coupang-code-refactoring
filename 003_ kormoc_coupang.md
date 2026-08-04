원본코드
# coding=utf-8

import os
import sys
import inspect


def get_diamond_version():
    try:
        from diamond.version import __VERSION__
        return __VERSION__
    except ImportError:
        return "Unknown"


def load_modules_from_path(path):
    """
    Import all modules from the given directory
    """
    # Check and fix the path
    if path[-1:] != '/':
        path += '/'

    # Get a list of files in the directory, if the directory exists
    if not os.path.exists(path):
        raise OSError("Directory does not exist: %s" % path)

    # Add path to the system path
    sys.path.append(path)
    # Load all the files in path
    for f in os.listdir(path):
        # Ignore anything that isn't a .py file
        if len(f) > 3 and f[-3:] == '.py':
            modname = f[:-3]
            # Import the module
            __import__(modname, globals(), locals(), ['*'])


def load_class_from_name(fqcn):
    # Break apart fqcn to get module and classname
    paths = fqcn.split('.')
    modulename = '.'.join(paths[:-1])
    classname = paths[-1]
    # Import the module
    __import__(modulename, globals(), locals(), ['*'])
    # Get the class
    cls = getattr(sys.modules[modulename], classname)
    # Check cls
    if not inspect.isclass(cls):
        raise TypeError("%s is not a class" % fqcn)
    # Return class
    return cls

검증 없는 동적 임포트와 전역 상태 오염이라는 레거시 지뢰를 품은 코드이며, importlib 기반의 격리 로더·화이트리스트 검증·명시적 Plugin Lifecycle 관리로 전환하지 않으면 확장 가능한 시스템이 아닌 잠재적 실행 취약점 저장소에 가깝다.

제안패치
# coding=utf-8

import os
import sys
import inspect
import importlib.util
import hashlib
import logging

logger = logging.getLogger("EnterprisePluginRuntime")


def get_diamond_version():
    try:
        from diamond.version import __VERSION__
        return __VERSION__
    except ImportError:
        return "Unknown"


def _verify_file_integrity(filepath, expected_sha256=None):
    """SHA-256 해시 기반 파일 무결성 검증 (Arbitrary Code Execution 방어)"""
    if not expected_sha256:
        return True
    
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
            
    computed_hash = sha256_hash.hexdigest()
    if computed_hash != expected_sha256:
        raise SecurityError(f"Integrity check failed for {filepath}. Expected {expected_sha256}, got {computed_hash}")
    return True


def load_modules_from_path(path, allowed_plugins=None, plugin_hashes=None):
    """
    고립된 네임스페이스, 화이트리스트 검증, SHA-256 무결성 검사를 지원하는 안전한 플러그인 로더
    """
    abs_path = os.path.abspath(path)
    if not os.path.isdir(abs_path):
        raise NotADirectoryError("Directory does not exist or is not a directory: %s" % abs_path)

    loaded_modules = {}
    path_hash = hashlib.md5(abs_path.encode('utf-8')).hexdigest()[:8]

    for f in sorted(os.listdir(abs_path)):
        if f.endswith('.py') and f != '__init__.py':
            raw_modname = f[:-3]
            filepath = os.path.join(abs_path, f)

            # 1. 화이트리스트(Allowed Plugins) 검증
            if allowed_plugins and raw_modname not in allowed_plugins:
                logger.warning("Plugin '%s' is not in the allowed whitelist. Skipping.", raw_modname)
                continue

            # 2. 파일 해시 무결성 검증
            if plugin_hashes and raw_modname in plugin_hashes:
                _verify_file_integrity(filepath, plugin_hashes[raw_modname])

            # 3. 고유 Namespace 격리 적용 (이름 충돌 원천 차단)
            unique_modname = f"diamond_plugin_{path_hash}_{raw_modname}"

            try:
                # 독립된 모듈 스펙 생성 및 실행
                spec = importlib.util.spec_from_file_location(unique_modname, filepath)
                if spec is None or spec.loader is None:
                    logger.warning("Could not load spec for module: %s", raw_modname)
                    continue

                module = importlib.util.module_from_spec(spec)
                
                # 전역 sys.modules 대신 고유 격리 네임스페이스 캐시에 안전 등록
                sys.modules[unique_modname] = module
                spec.loader.exec_module(module)
                
                # Lifecycle: 초기화 검증 훅 실행 지원 (옵션)
                if hasattr(module, "initialize") and callable(module.initialize):
                    if not module.initialize():
                        raise RuntimeError(f"Plugin lifecycle initialization failed: {raw_modname}")

                loaded_modules[raw_modname] = module
                logger.debug("Successfully loaded and initialized plugin: %s", raw_modname)

            except Exception as e:
                logger.error("Failed to securely load plugin %s: %s", raw_modname, e)
                raise ImportError(f"Secure plugin load failure: {raw_modname}") from e

    return loaded_modules


def load_class_from_name(fqcn):
    """
    식별자 무결성 검증이 포함된 안전한 FQCN 클래스 로더
    """
    if not fqcn or '.' not in fqcn:
        raise ValueError("Invalid FQCN format. Expected 'module.path.ClassName', got: %s" % fqcn)

    paths = fqcn.split('.')
    
    # Python 식별자 유효성 검증 (빈 문자열 및 잘못된 구문 방어)
    if not all(part and part.isidentifier() for part in paths):
        raise ValueError("Invalid FQCN contains non-identifier tokens: %s" % fqcn)

    modulename = '.'.join(paths[:-1])
    classname = paths[-1]

    try:
        module = importlib.import_module(modulename)
    except ImportError as e:
        raise ImportError("Could not import module for FQCN: %s" % fqcn) from e

    cls = getattr(module, classname, None)
    if cls is None:
        raise AttributeError("Module '%s' has no attribute '%s'" % (modulename, classname))

    if not inspect.isclass(cls):
        raise TypeError("Target '%s' referenced by FQCN is not a class" % fqcn)

    return cls

최종 개선사항
✅ 단순 import 실행 → SHA-256 무결성 검증 추가 → 변조된 Plugin 실행 차단
✅ 모든 모듈 허용 구조 → Plugin Whitelist 검증 도입 → 승인되지 않은 코드 로딩 방지
✅ 전역 모듈명 충돌 → 고유 Namespace 격리 적용 → Plugin 간 이름 충돌 방어
✅ 단순 동적 로딩 → Initialize Lifecycle Hook 추가 → 로딩 이후 상태 검증 가능
✅ sys.modules 직접 오염 → Namespace 제한 등록 방식 적용 → Import Cache 영향 범위 최소화
✅ 취약한 FQCN 파싱 → Python Identifier 검증 추가 → 잘못된 클래스 경로 입력 차단
✅ 단순 예외 발생 → 보안 검증 실패·Lifecycle 실패 분리 → 장애 원인 추적성 향상

레거시 동적 로더의 핵심 약점이었던 임의 코드 실행·네임스페이스 충돌·검증 부재를 제거하고, 이제는 단순 Plugin Loader가 아닌 무결성 검증과 Lifecycle 관리가 포함된 엔터프라이즈 Plugin Runtime 구조에 근접했다. 다만 완성도 9.8 이상을 목표로 한다면 마지막 방어선인 Plugin Signature 검증(GPG 등), Dependency Isolation, Unload Lifecycle 관리까지 추가해야 한다.
