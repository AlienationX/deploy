#!/usr/bin/env python3
# coding: utf-8

# deploy trino

import os
import logging
from fabric.connection import Connection


# ###########################################################
SERVER_HOST = "hadoop-01"
SERVER_PORT = 22022
SERVER_USER = "root"
SERVER_PWD = "20200114@8_4^@S9HM23^@DnnXd9^sl%97%g9e"

AGENT_HOST = "hadoop-02"
AGENT_PORT = 22022
AGENT_USER = "root"
AGENT_PWD = "20200114@8_4^@S9HM23^@DnnXd9^sl%97%g9e"

MASTER_HOST = "hadoop-prod01"
NTP_HOST = "10.63.90.227"


def create_logger(name):
    # set default logging configuration
    logger = logging.getLogger(name)  # initialize logging class
    logger.setLevel(logging.DEBUG)  # default log level
    format_str = logging.Formatter("%(asctime)s %(levelname)s - %(name)s[%(lineno)d]: %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(format_str)

    logger.addHandler(stream_handler)
    return logger


class Deploy:

    def __init__(self):
        self.logger = create_logger(__name__)
        self.server = Connection(host=SERVER_HOST, user=SERVER_USER, port=SERVER_PORT, connect_kwargs={"password": SERVER_PWD})
        self.agent = Connection(host=AGENT_HOST, user=AGENT_USER, port=AGENT_PORT, connect_kwargs={"password": AGENT_PWD})

    def create_authorized_keys(self):
        """"生成公钥"""
        cmd = """
        ssh-keygen &&
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys &&
        chmod 600 ~/.ssh/authorized_keys
        """
        self.logger.info(cmd)
        self.server.run(cmd, encoding="utf-8")

    def distribute_authorized_keys(self):
        """分发公钥，免密登录"""
        cmd1 = "mkdir -p ~/.ssh"
        self.logger.info(cmd1)
        self.agent.run(cmd1)
        # ssh-copy-id需要输入密码，不推荐
        # cmd2 = "ssh-copy-id root@{}".format(AGENT_HOST)
        pub_key = self.server.run("cat ~/.ssh/id_rsa.pub", hide=True).stdout
        cmd2 = "echo '{}' >> ~/.ssh/authorized_keys".format(pub_key)
        self.logger.info(cmd2)
        self.agent.run(cmd2)

    def distribute_soft(self):
        """分发软件"""
        files = [
            "/home/soft/jdk-17_linux-x64_bin.tar.gz",
            "/home/soft/trino/trino-server-402.tar.gz",
            "/home/soft/trino/trino-cli-402-executable.jar",
        ]
        soft_path = "/home/soft"
        cmd_str = "scp -P{port} {file} {user}@{url}:{soft_path}"
        for file in files:
            soft_path = os.path.dirname(file)
            cmd = cmd_str.format(port=AGENT_PORT, file=file, user=AGENT_USER, url=AGENT_HOST, soft_path=soft_path)
            self.logger.info(cmd)
            self.agent.run("mkdir -p {}".format(soft_path))
            self.server.run(cmd, encoding="utf-8")
        self.logger.info("send software done.")
        
    def add_user(self):
        cmd = """
        useradd trino
        usermod trino -G hive
        """
        self.logger.info(cmd)
        self.agent.run(cmd, encoding="utf-8")
        message = self.agent.run("id trino", encoding="utf-8", hide=True).stdout
        self.logger.info(message)
        
        
    def install_java(self):
        """部署java, su - trino 会卡住, 必须使用这种方式 echo 'whoami' | su - trino """
        cmd = """
su - trino -c "
whoami &&
# tar -zxvf /home/soft/jdk-17_linux-x64_bin.tar.gz -C /home/trino &&
# 去掉v, 不显示过程解压过程
tar -zxf /home/soft/jdk-17_linux-x64_bin.tar.gz -C /home/trino &&
echo '
JAVA_HOME=\\"/home/trino/jdk-17.0.5\\"
PATH=\\"\$JAVA_HOME/bin:\$PATH\\"
export JAVA_HOME PATH
' >> ~/.bash_profile &&
source ~/.bash_profile &&
java -version
"
"""
        self.logger.info(cmd) 
        self.agent.run(cmd, encoding="utf-8")

    def install_trino(self):
        """解压并配置"""
        cmd = """
su - trino -c "

mkdir -p ~/data
TRINO_VERSION=402
echo \${TRINO_VERSION}

tar -zxf /home/soft/trino/trino-server-\${TRINO_VERSION}.tar.gz -C .
ln -s trino-server-\${TRINO_VERSION} trino-server

# 增加命令行工具，把trino-cli-${TRINO_VERSION}.jar复制到/home/app/trino/trino-server-${TRINO_VERSION}/bin/目录下
cp /home/soft/trino/trino-cli-\${TRINO_VERSION}-executable.jar trino-server/bin/trino-cli
chmod +x trino-server/bin/trino-cli

mkdir -p trino-server/etc
cd trino-server/etc

echo '-server
-Xmx16G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-XX:+UnlockDiagnosticVMOptions
-XX:+UseAESCTRIntrinsics' > jvm.config
# -DHADOOP_USER_NAME=hive
# 默认使用启动trino的用户访问hadoop

# 集群部署-coordinator
# echo 'coordinator=true
# node-scheduler.include-coordinator=false
# http-server.http.port=8081
# query.max-memory=50GB
# query.max-memory-per-node=1GB
# discovery.uri=http://hadoop-03:8081' > config.properties

# 集群部署-workers
echo 'coordinator=false
http-server.http.port=8081
query.max-memory=50GB
query.max-memory-per-node=1GB
discovery.uri=http://hadoop-03:8081' > config.properties

echo 'node.environment=production
# node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.id=trino-worker01
node.data-dir=/home/trino/data' > node.properties

echo 'io.trino=INFO' > log.properties
"
"""
        
        self.logger.info(cmd)
        self.agent.run(cmd, encoding="utf-8")

    def add_catalog(self):
        """增加catalog"""
        cmd = """
su - trino -c "
mkdir -p trino-server/etc/catalog
cd trino-server/etc/catalog

echo 'connector.name=hive-hadoop2
hive.metastore.uri=thrift://hadoop-01:9083
# hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml # 可以不配置
' > hive.properties

echo 'connector.name=mysql
connection-url=jdbc:mysql://hadoop-01:3306??useUnicode=true&characterEncoding=UTF-8&useSSL=false&tinyInt1isBit=false
connection-user=hadoop
connection-password=Hadoop123!@#
' > mysql.properties
"
        """
        self.agent.run(cmd, encoding="utf-8")
        
    def add_password():
        """"""
    
    def add_authentication():
        """"""
        
    def start(self):
        cmd = """su - trino -c "trino-server/bin/launcher restart" """
        self.agent.run(cmd, encoding="utf-8")


if __name__ == "__main__":
    d = Deploy()
    d.agent.run("hostname; hostname -i")
    # d.create_authorized_keys()            # 服务端需要执行
    # d.distribute_authorized_keys()
    # d.distribute_soft()
    # d.add_user()
    # d.install_java()
    
    d.install_trino()
    d.add_catalog()
    d.start()

    d.logger.info("{} deploy successful.".format(AGENT_HOST))