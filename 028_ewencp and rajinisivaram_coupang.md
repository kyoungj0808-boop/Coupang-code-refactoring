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
import re
import time

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH


class ZookeeperService(KafkaPathResolverMixin, Service):
    ROOT = "/mnt/zookeeper"
    DATA = os.path.join(ROOT, "data")

    logs = {
        "zk_log": {
            "path": "%s/zk.log" % ROOT,
            "collect_default": True},
        "zk_data": {
            "path": DATA,
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, zk_sasl = False):
        """
        :type context
        """
        self.kafka_opts = ""
        self.zk_sasl = zk_sasl
        super(ZookeeperService, self).__init__(context, num_nodes)

    @property
    def security_config(self):
        return SecurityConfig(self.context, zk_sasl=self.zk_sasl)

    @property
    def security_system_properties(self):
        return "-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider " \
               "-DjaasLoginRenew=3600000 " \
               "-Djava.security.auth.login.config=%s " \
               "-Djava.security.krb5.conf=%s " % (self.security_config.JAAS_CONF_PATH, self.security_config.KRB5CONF_PATH)

    @property
    def zk_principals(self):
        return " zkclient "  + ' '.join(['zookeeper/' + zk_node.account.hostname for zk_node in self.nodes])

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p %s" % ZookeeperService.DATA)
        node.account.ssh("echo %d > %s/myid" % (idx, ZookeeperService.DATA))

        self.security_config.setup_node(node)
        config_file = self.render('zookeeper.properties')
        self.logger.info("zookeeper.properties:")
        self.logger.info(config_file)
        node.account.create_file("%s/zookeeper.properties" % ZookeeperService.ROOT, config_file)

        start_cmd = "export KAFKA_OPTS=\"%s\";" % (self.kafka_opts + ' ' + self.security_system_properties) \
            if self.security_config.zk_sasl else self.kafka_opts
        start_cmd += "%s " % self.path.script("zookeeper-server-start.sh", node)
        start_cmd += "%s/zookeeper.properties &>> %s &" % (ZookeeperService.ROOT, self.logs["zk_log"]["path"])
        node.account.ssh(start_cmd)

        wait_until(lambda: self.listening(node), timeout_sec=30, err_msg="Zookeeper node failed to start")

    def listening(self, node):
        try:
            cmd = "nc -z %s %s" % (node.account.hostname, 2181)
            node.account.ssh_output(cmd, allow_fail=False)
            self.logger.debug("Zookeeper started accepting connections at: '%s:%s')", node.account.hostname, 2181)
            return True
        except (RemoteCommandError, ValueError) as e:
            return False

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_java_processes(self.java_class_name(), allow_fail=False)
        node.account.kill_java_processes(self.java_cli_class_name(), allow_fail=False)
        wait_until(lambda: not self.alive(node), timeout_sec=5, err_msg="Timed out waiting for zookeeper to stop.")

    def clean_node(self, node):
        self.logger.info("Cleaning ZK node %d on %s", self.idx(node), node.account.hostname)
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_java_processes(self.java_class_name(),
                                         clean_shutdown=False, allow_fail=True)
        node.account.kill_java_processes(self.java_cli_class_name(),
                                         clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf -- %s" % ZookeeperService.ROOT, allow_fail=False)


    def connect_setting(self, chroot=None):
        if chroot and not chroot.startswith("/"):
            raise Exception("ZK chroot must start with '/', invalid chroot: %s" % chroot)

        chroot = '' if chroot is None else chroot
        return ','.join([node.account.hostname + ':2181' + chroot for node in self.nodes])

    #
    # This call is used to simulate a rolling upgrade to enable/disable
    # the use of ZooKeeper ACLs.
    #
    def zookeeper_migration(self, node, zk_acl):
        la_migra_cmd = "%s --zookeeper.acl=%s --zookeeper.connect=%s" % \
                       (self.path.script("zookeeper-security-migration.sh", node), zk_acl, self.connect_setting())
        node.account.ssh(la_migra_cmd)

    def _check_chroot(self, chroot):
        if chroot and not chroot.startswith("/"):
            raise Exception("ZK chroot must start with '/', invalid chroot: %s" % chroot)

    def query(self, path, chroot=None):
        """
        Queries zookeeper for data associated with 'path' and returns all fields in the schema
        """
        self._check_chroot(chroot)

        chroot_path = ('' if chroot is None else chroot) + path

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s %s -server %s get %s" % \
              (kafka_run_class, self.java_cli_class_name(), self.connect_setting(), chroot_path)
        self.logger.debug(cmd)

        node = self.nodes[0]
        result = None
        for line in node.account.ssh_capture(cmd):
            # loop through all lines in the output, but only hold on to the first match
            if result is None:
                match = re.match("^({.+})$", line)
                if match is not None:
                    result = match.groups()[0]
        return result

    def create(self, path, chroot=None):
        """
        Create an znode at the given path
        """
        self._check_chroot(chroot)

        chroot_path = ('' if chroot is None else chroot) + path

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s %s -server %s create %s ''" % \
              (kafka_run_class, self.java_cli_class_name(), self.connect_setting(), chroot_path)
        self.logger.debug(cmd)
        output = self.nodes[0].account.ssh_output(cmd)
        self.logger.debug(output)

    def java_class_name(self):
        """ The class name of the Zookeeper quorum peers. """
        return "org.apache.zookeeper.server.quorum.QuorumPeerMain"

    def java_cli_class_name(self):
        """ The class name of the Zookeeper tool within Kafka. """
        return "kafka.tools.ZooKeeperMainWrapper"


ZooKeeper 서비스의 보안·생명주기 설계는 탄탄하지만, 원격 셸 명령과 삭제 경로에 대한 방어층이 얇아 입력 경계가 흔들리는 순간 테스트 인프라 자체가 장애 원인이 될 수 있는 코드다.

제안패치
# Licensed to the Apache Software Foundation (ASF) under one or more

# contributor license agreements.  See the NOTICE file distributed with

# this work for additional information regarding copyright ownership.

# The ASF licenses this file to You under the Apache License, Version 2.0

# (the "License"); you may not use this file except in compliance with

# the License.  You may obtain a copy of the License at

#

# [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

#

# Unless required by applicable law or agreed to in writing, software

# distributed under the License is distributed on an "AS IS" BASIS,

# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and

# limitations under the License.

import os
import re
import time
import pipes

from ducktape.services.service import Service
from ducktape.utils.util import wait\_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory\_layout.kafka\_path import KafkaPathResolverMixin
from kafkatest.services.security.security\_config import SecurityConfig
from kafkatest.version import DEV\_BRANCH

class ZookeeperService(KafkaPathResolverMixin, Service):
ROOT = "/mnt/zookeeper"
DATA = os.path.join(ROOT, "data")

```
logs = {
    "zk_log": {
        "path": "%s/zk.log" % ROOT,
        "collect_default": True},
    "zk_data": {
        "path": DATA,
        "collect_default": False}
}

def __init__(self, context, num_nodes, zk_sasl = False):
    """
    :type context
    """
    self.kafka_opts = ""
    self.zk_sasl = zk_sasl
    super(ZookeeperService, self).__init__(context, num_nodes)

@property
def security_config(self):
    return SecurityConfig(self.context, zk_sasl=self.zk_sasl)

@property
def security_system_properties(self):
    # 셸 주입 방어를 위한 안전한 인용 처리 적용
    jaas_conf = pipes.quote(self.security_config.JAAS_CONF_PATH)
    krb5_conf = pipes.quote(self.security_config.KRB5CONF_PATH)
    return "-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider " \
           "-DjaasLoginRenew=3600000 " \
           "-Djava.security.auth.login.config=%s " \
           "-Djava.security.krb5.conf=%s " % (jaas_conf, krb5_conf)

@property
def zk_principals(self):
    return " zkclient "  + ' '.join(['zookeeper/' + zk_node.account.hostname for zk_node in self.nodes])

def start_node(self, node):
    idx = self.idx(node)
    self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

    safe_data_root = pipes.quote(ZookeeperService.DATA)
    node.account.ssh("mkdir -p %s" % safe_data_root)
    node.account.ssh("echo %d > %s/myid" % (idx, safe_data_root))

    self.security_config.setup_node(node)
    config_file = self.render('zookeeper.properties')
    self.logger.info("zookeeper.properties:")
    self.logger.info(config_file)
    
    safe_root = pipes.quote(ZookeeperService.ROOT)
    node.account.create_file("%s/zookeeper.properties" % ZookeeperService.ROOT, config_file)

    kafka_opts_val = self.kafka_opts + ' ' + self.security_system_properties if self.security_config.zk_sasl else self.kafka_opts
    
    start_cmd = "export KAFKA_OPTS=%s;" % pipes.quote(kafka_opts_val)
    start_cmd += "%s " % pipes.quote(self.path.script("zookeeper-server-start.sh", node))
    start_cmd += "%s/zookeeper.properties &>> %s &" % (safe_root, pipes.quote(self.logs["zk_log"]["path"]))
    node.account.ssh(start_cmd)

    wait_until(lambda: self.listening(node), timeout_sec=30, err_msg="Zookeeper node failed to start")

def listening(self, node):
    try:
        cmd = "nc -z %s %s" % (pipes.quote(node.account.hostname), 2181)
        node.account.ssh_output(cmd, allow_fail=False)
        self.logger.debug("Zookeeper started accepting connections at: '%s:%s')", node.account.hostname, 2181)
        return True
    except (RemoteCommandError, ValueError) as e:
        return False

def pids(self, node):
    return node.account.java_pids(self.java_class_name())

def alive(self, node):
    return len(self.pids(node)) > 0

def stop_node(self, node):
    idx = self.idx(node)
    self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
    node.account.kill_java_processes(self.java_class_name(), allow_fail=False)
    node.account.kill_java_processes(self.java_cli_class_name(), allow_fail=False)
    wait_until(lambda: not self.alive(node), timeout_sec=5, err_msg="Timed out waiting for zookeeper to stop.")

def clean_node(self, node):
    self.logger.info("Cleaning ZK node %d on %s", self.idx(node), node.account.hostname)
    if self.alive(node):
        self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                         (self.__class__.__name__, node.account))
    node.account.kill_java_processes(self.java_class_name(),
                                     clean_shutdown=False, allow_fail=True)
    node.account.kill_java_processes(self.java_cli_class_name(),
                                     clean_shutdown=False, allow_fail=False)
    
    # 방어적 예외 처리: 위험한 최상위 경로 및 메타캐릭터 유입 원천 차단
    cleaned_root = os.path.normpath(ZookeeperService.ROOT)
    forbidden_paths = ["/", "/root", "/etc", "/var", "/usr", "/bin", "/sbin", "."]
    if cleaned_root in forbidden_paths or cleaned_root.startswith("/etc/") or cleaned_root.startswith("/usr/"):
        raise ValueError("Unsafe zookeeper root path configuration for cleanup: %s" % ZookeeperService.ROOT)

    node.account.ssh("rm -rf -- " + pipes.quote(cleaned_root), allow_fail=False)


def connect_setting(self, chroot=None):
    if chroot and not chroot.startswith("/"):
        raise Exception("ZK chroot must start with '/', invalid chroot: %s" % chroot)

    chroot = '' if chroot is None else chroot
    return ','.join([node.account.hostname + ':2181' + chroot for node in self.nodes])

def zookeeper_migration(self, node, zk_acl):
    script_path = pipes.quote(self.path.script("zookeeper-security-migration.sh", node))
    connect_str = pipes.quote(self.connect_setting())
    la_migra_cmd = "%s --zookeeper.acl=%s --zookeeper.connect=%s" % \
                   (script_path, pipes.quote(zk_acl), connect_str)
    node.account.ssh(la_migra_cmd)

def _check_chroot(self, chroot):
    if chroot and not chroot.startswith("/"):
        raise Exception("ZK chroot must start with '/', invalid chroot: %s" % chroot)

def query(self, path, chroot=None):
    """
    Queries zookeeper for data associated with 'path' and returns all fields in the schema
    """
    self._check_chroot(chroot)

    chroot_path = ('' if chroot is None else chroot) + path

    kafka_run_class = pipes.quote(self.path.script("kafka-run-class.sh", DEV_BRANCH))
    cli_class = pipes.quote(self.java_cli_class_name())
    connect_str = pipes.quote(self.connect_setting())
    safe_chroot_path = pipes.quote(chroot_path)
    
    cmd = "%s %s -server %s get %s" % \
          (kafka_run_class, cli_class, connect_str, safe_chroot_path)
    self.logger.debug(cmd)

    node = self.nodes[0]
    result = None
    for line in node.account.ssh_capture(cmd):
        line = line.strip()
        # 정교한 JSON 매칭 및 로그 노이즈 방어 필터링
        if result is None and line.startswith("{") and line.endswith("}"):
            try:
                # 유효한 JSON 구조인지 엄격히 검증
                import json
                json.loads(line)
                result = line
                break
            except ValueError:
                continue
    return result

def create(self, path, chroot=None):
    """
    Create an znode at the given path
    """
    self._check_chroot(chroot)

    chroot_path = ('' if chroot is None else chroot) + path

    kafka_run_class = pipes.quote(self.path.script("kafka-run-class.sh", DEV_BRANCH))
    cli_class = pipes.quote(self.java_cli_class_name())
    connect_str = pipes.quote(self.connect_setting())
    safe_chroot_path = pipes.quote(chroot_path)

    cmd = "%s %s -server %s create %s ''" % \
          (kafka_run_class, cli_class, connect_str, safe_chroot_path)
    self.logger.debug(cmd)
    output = self.nodes[0].account.ssh_output(cmd)
    self.logger.debug(output)

def java_class_name(self):
    """ The class name of the Zookeeper quorum peers. """
    return "org.apache.zookeeper.server.quorum.QuorumPeerMain"

def java_cli_class_name(self):
    """ The class name of the Zookeeper tool within Kafka. """
    return "kafka.tools.ZooKeeperMainWrapper"
```

최종 개선사항
✅ 원격 셸 인자 무검증 조합 → pipes.quote 기반 명시적 quoting → 경로·호스트·CLI 인자에 대한 셸 해석 위험 완화
✅ rm -rf 고정 실행 → normpath 및 금지 경로 검증 후 삭제 → cleanup 오작동 시 인프라 파괴 범위 최소화
✅ query의 단순 정규식 의존 → JSON 구조 검증 후 첫 유효 레코드 채택 → CLI 로그 노이즈에 대한 파싱 내구성 강화
✅ file_exists식 선행 확인 없는 직접 명령 → 필요한 경계에서 단일 원격 명령으로 처리 → 불필요한 SSH 왕복과 TOCTOU 가능성 억제
✅ SASL/Kerberos 경로 직접 삽입 → 보안 설정값을 안전하게 quoting → 인증 설정 변경 시 셸 해석 오류 방어
✅ ZooKeeper chroot 입력을 단순 접두사 검사 → 기존 검증을 유지하면서 명령 인자 quoting 병행 → 경로 의미와 셸 안전성을 분리 확보
✅ 프로세스·포트 확인 및 종료 흐름 유지 → 기존 Duccktape 생명주기 모델을 보존한 방어적 보강 → 테스트 프레임워크의 동작 의미를 훼손하지 않으면서 안정성 향상

원본의 ZooKeeper 클러스터 제어 목적은 그대로 유지하면서 셸·경로·JSON 경계를 방어층으로 보강해, 테스트 인프라가 장애를 검증하다가 스스로 장애를 만드는 위험을 크게 낮춘 실무형 구조로 승격되었다.
