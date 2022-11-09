#!/usr/bin/env python3
# coding: utf-8

# deploy cdh 5.16.2

import os
import logging
from fabric import Connection


# ###########################################################
MIDDLE_HOST = "10.63.82.202"
MIDDLE_PORT = 22022
MIDDLE_USER = "root"
MIDDLE_PWD = "123456"

SERVER_HOST = "10.63.82.202"
SERVER_PORT = 22022
SERVER_USER = "root"
SERVER_PWD = "123456"

AGENT_HOST = "10.63.82.218"
AGENT_PORT = 22022
AGENT_USER = "root"
AGENT_PWD = "123456"

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
        # mid_conn = Connection(host=MIDDLE_HOST, user=MIDDLE_USER, port=MIDDLE_PORT, connect_kwargs={"password": MIDDLE_PWD})
        # self.server = Connection(host=SERVER_HOST, user=SERVER_USER, port=SERVER_PORT, connect_kwargs={"password": SERVER_PWD}, gateway=mid_conn)
        # self.agent = Connection(host=AGENT_HOST, user=AGENT_USER, port=AGENT_PORT, connect_kwargs={"password": AGENT_PWD}, gateway=mid_conn)
        self.current_hosts = """
10.63.82.198 hadoop-prod01
10.63.82.199 hadoop-prod02
10.63.82.200 hadoop-prod03
10.63.82.201 hadoop-prod04
10.63.82.202 hadoop-prod05
10.63.82.203 hadoop-prod06
10.63.82.204 hadoop-prod07
10.63.82.205 hadoop-prod08
10.63.82.191 hadoop-prod09
10.63.82.192 hadoop-prod10
10.63.82.212 hadoop-prod11
10.63.82.213 hadoop-prod12

10.63.82.207 hadoop-prod13
10.63.82.208 hadoop-prod14
10.63.82.209 hadoop-prod15

10.63.80.104 hadoop-dev01
10.63.80.105 hadoop-dev02
10.63.80.106 hadoop-dev03
10.63.80.107 hadoop-dev04
10.63.80.108 hadoop-dev05
"""
        self.new_hosts = """
10.63.82.198 hadoop-prod01
10.63.82.199 hadoop-prod02
10.63.82.200 hadoop-prod03
10.63.82.201 hadoop-prod04
10.63.82.202 hadoop-prod05
10.63.82.203 hadoop-prod06
10.63.82.204 hadoop-prod07
10.63.82.205 hadoop-prod08
10.63.82.191 hadoop-prod09
10.63.82.192 hadoop-prod10
10.63.82.212 hadoop-prod11
10.63.82.213 hadoop-prod12

10.63.82.207 hadoop-prod13
10.63.82.208 hadoop-prod14
10.63.82.209 hadoop-prod15
10.63.82.214 hadoop-prod16
10.63.82.215 hadoop-prod17
10.63.82.216 hadoop-prod18
10.63.82.217 hadoop-prod19
10.63.82.218 hadoop-prod20

10.63.80.104 hadoop-dev01
10.63.80.105 hadoop-dev02
10.63.80.106 hadoop-dev03
10.63.80.107 hadoop-dev04
10.63.80.108 hadoop-dev05
"""

    def add_host(self):
        """
        修改主机名，agent主机增加当前hosts
        vi /etc/hostname
        vi /etc/hosts
        """
        cmd = "echo '{}' >> /etc/hosts".format(self.current_hosts)
        self.logger.info(cmd)
        self.agent.run(cmd, encoding="utf-8")

    def replace_all_hosts(self):
        """所有主机替换成新的hosts"""
        host_list = [x for x in self.new_hosts.split("\n") if "prod" in x]
        for host_line in host_list:
            host = host_line.split()[0]
            cmd = """ ssh -p{port} root@{host} "cat /etc/hosts" """.format(port=SERVER_PORT, host=host)
            hosts_content = self.server.run(cmd, encoding="utf-8", hide="both").stdout.replace(self.current_hosts, self.new_hosts)
            cmd1 = """ ssh -p{port} root@{host} "echo '{hosts_content}' > /etc/hosts" """.format(port=SERVER_PORT, host=host, hosts_content=hosts_content)
            self.logger.info(cmd1)
            self.server.run(cmd1, encoding="utf-8")
            self.logger.info("{host} replace new hosts done.".format(host=host))

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
        pub_key = self.server.run("cat ~/.ssh/id_rsa.pub", hide=True).stdout
        cmd2 = "echo '{}' >> ~/.ssh/authorized_keys".format(pub_key)
        self.logger.info(cmd2)
        self.agent.run(cmd2)

    def distribute_soft(self):
        """分发软件"""
        files = [
            "/home/soft/jdk-8u271-linux-x64.tar.gz",
            "/home/soft/mysql-connector-java-5.1.48.jar",
            "/home/soft/cdh5.16.2/cloudera-manager-centos7-cm5.16.2_x86_64.tar.gz",
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

    def install_java(self):
        """部署java"""
        cmd = """
yum -y install perl ntp httpd mod_ssl gcc gcc-c++ python-devel libxslt-devel cyrus-sasl* psmisc &&

mkdir -p /usr/java &&
# tar -zxvf /home/soft/jdk-8u271-linux-x64.tar.gz -C /usr/java &&
# 去掉v，不显示过程解压过程
tar -zxf /home/soft/jdk-8u271-linux-x64.tar.gz -C /usr/java &&

echo '
JAVA_HOME="/usr/java/jdk1.8.0_271"
PATH="$PATH:$JAVA_HOME/bin"
export JAVA_HOME PATH
' >> /etc/profile &&
source /etc/profile &&
java -version
        """
        self.logger.info(cmd)
        self.agent.run(cmd, encoding="utf-8")

    def set_base_config(self):
        """基础配置
        1.关闭SELINUX
            setenforce 0 （临时生效）  
            修改 /etc/selinux/config 下的 SELINUX=disabled （重启后永久生效）
        2.关闭防火墙
            强烈推荐不关闭防火墙，严格限制端口开放，但是需要整理端口，后续完善
        """

        cmd = """
systemctl status iptables
systemctl stop iptables
echo never > /sys/kernel/mm/transparent_hugepage/defrag
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo 0 > /proc/sys/vm/swappiness

echo '
# cdh
echo never > /sys/kernel/mm/transparent_hugepage/defrag
echo never > /sys/kernel/mm/transparent_hugepage/enabled
' >> /etc/rc.local
        """
        self.logger.info(cmd)
        self.agent.run(cmd, encoding="utf-8")

    def set_ntp(self):
        """
        配置NTP（Network Time Protocol）服务，时区同步问题
            yum install ntp -y
            vi /etc/ntp.conf            # 注释掉默认的地址(21行至24行)，增加: server 10.63.90.227
            ntpdate 10.63.90.227        # 手动同步下时间，然后再启动服务
            service ntpd start
            ntpdc -c loopinfo           # offset为0s即可
        """
        cmd = """
        yum install ntp -y
        # vi /etc/ntp.conf

        # 增加ntp的server地址
        sed -i "/server 3.centos.pool.ntp.org iburst/a\server {npt_host}" /etc/ntp.conf
        # 注释默认的server
        sed -i "s/server 0.centos.pool.ntp.org iburst/# server 0.centos.pool.ntp.org iburst/g" /etc/ntp.conf
        sed -i "s/server 1.centos.pool.ntp.org iburst/# server 0.centos.pool.ntp.org iburst/g" /etc/ntp.conf
        sed -i "s/server 2.centos.pool.ntp.org iburst/# server 0.centos.pool.ntp.org iburst/g" /etc/ntp.conf
        sed -i "s/server 3.centos.pool.ntp.org iburst/# server 0.centos.pool.ntp.org iburst/g" /etc/ntp.conf

        ntpdate {npt_host}
        service ntpd start
        ntpdc -c loopinfo
        """.format(npt_host=NTP_HOST)
        self.logger.info(cmd)
        self.agent.run(cmd, encoding="utf-8")

    def install_mysql(self):
        """部署mysql"""
        # 解压
        cmd = "tar -zxvf /home/soft/mysql-5.7.27-linux-glibc2.12-x86_64.tar.gz -C /home/app"
        # 创建软链接
        cmd1 = "cd /home/app && ln -s mysql-5.7.27-linux-glibc2.12-x86_64 mysql"
        # 添加环境变量
        cmd2 = "vi /etc/profile"
        # 添加配置文件和数据目录 cp /home/app/mysql/support-files/my-default.cnf /etc/my.cnf
        cmd3 = "vi /etc/my.cnf; cd /home/app/mysql && mkdir data"
        """需要创建mysql用户,并将data目录授权给mysql用户
        # groupadd mysql
        # useradd -r -g mysql -M mysql
        useradd mysql
        chown -R mysql:mysql /home/app/mysql/data

        # 设置初始密码为空
        mysqld --initialize-insecure --user=mysql
        """
        # 开机自动启
        cmd4 = "cp /home/app/mysql/support-files/mysql.server /etc/init.d/mysql; chkconfig --add mysql"
        cmd5 = "service mysql start"
        # 启动，登录无需输入密码，进去后设置root密码。
        # mysql -uroot -p
        # set password=password('root');
        
    def deploy_cdh_server(self):
        """
        deploy cdh server
        tar -zxvf /home/soft/cdh5.16.2/cloudera-manager-centos7-cm5.16.2_x86_64.tar.gz -C /home/app &&
        ln -s /home/app/cm-5.16.2 /opt/cm-5.16.2 &&
        ln -s /home/app/cloudera /opt/cloudera
        1. cm数据库配置
            # 创建cm的资料库，cm迁移可以修改下面的数据库配置
            /opt/cm-5.16.2/etc/cloudera-scm-server/db.properties
            yum或者6.3.2版本等的配置路径是/etc/cloudera-scm-server/db.properties
        2. jdbc配置
            # cm
            cp /home/soft/mysql-connector-java-5.1.48.jar /opt/cm-5.16.2/share/cmf/lib/

            # cloudera，改目录只有部署完agent才会生成
            cp /home/soft/mysql-connector-java-5.1.48.jar /opt/cloudera/parcels/CDH/jars/
            cd /opt/cloudera/parcels/CDH/lib/sentry/lib && ln -s ../../../jars/mysql-connector-java-5.1.48.jar mysql-connector-java-5.1.48.jar
            cd /opt/cloudera/parcels/CDH/lib/hive/lib && ln -s ../../../jars/mysql-connector-java-5.1.48.jar mysql-connector-java-5.1.48.jar
            cd ...hue...
            # （其实是/var/lib/oozie，所有不能使用相对路径.代替，强烈建议使用cp直接复制）
            cd /opt/cloudera/parcels/CDH/lib/oozie/libext && cp /home/soft/mysql-connector-java-5.1.48.jar mysql-connector-java-5.1.48.jar

            # sqoop

        """
        cmd = "cp /home/soft/mysql-connector-java-5.1.48.jar /opt/cm-5.16.2/share/cmf/lib/ && ls /opt/cm-5.16.2/share/cmf/lib/ | grep mysql"
        cmd1 = "/opt/cm-5.16.2/share/cmf/schema/scm_prepare_database.sh mysql cm -hlocalhost -uroot -p'root123' --scm-host localhost scm scm scm" # cm库不需要手动创建，会自动创建，提前创建会报错
        cmd2 = """
        cp /home/soft/cdh5.16.2/CDH-5.16.2-1.cdh5.16.2.p0.8-el7.parcel /opt/cloudera/parcel-repo
        cp /home/soft/cdh5.16.2/CDH-5.16.2-1.cdh5.16.2.p0.8-el7.parcel.sha1 /opt/cloudera/parcel-repo/CDH-5.16.2-1.cdh5.16.2.p0.8-el7.parcel.sha
        cp /home/soft/cdh5.16.2/manifest.json /opt/cloudera/parcel-repo
        """
        cmd3 = "/opt/cm-5.16.2/etc/init.d/cloudera-scm-server start"
    
        self.logger.info(cmd)
        self.logger.info(cmd1)
        """
        初次安装强烈建议只安装HDFS、YARN、Zookeeper、Hive、Spark、Impala、Hue
        cm页面安装部署完各个组件的时候如果失败，如下步骤可供参考：
        1. HDFS的dataname需要手动格式化，创建/tmp目录(/tmp)
        2. YARN需要手动创建作业历史记录目录(/user/history)、NodeManager远程应用程序日志目录(/tmp/logs)
        3. Hive需要手动创建Hive用户(hive), Hive目录(/user/hive/warehouse), Hive Metastore(mysql), 修改mysql元数据的字符集为utf-8
        4. Spark需要Install Spark JAR, Create Spark User Dir(/user/spark), Create Spark History Log Dir(/user/spark/applicationHistory)
        5. Impala需要手动创建Impala用户目录(/user/impala)
        6. HBase需要手动创建根目录(/hbase)
        7. Oozie需要安装Oozie共享库(创建/user/oozie目录并上载相关文件), 创建Oozie数据库(mysql)
        """

    def deploy_cdh_agent(self):
        """部署cdh"""
        cmd = """
        mkdir -p /home/app/dfs
        rm -rf /dfs
        ln -s /home/app/dfs /dfs

        mkdir -p /home/app/yarn
        rm -rf /yarn
        ln -s /home/app/yarn /yarn

        mkdir -p /home/app/data
        rm -rf /data
        ln -s /home/app/data /data

        # tar -zxvf /home/soft/cdh5.16.2/cloudera-manager-centos7-cm5.16.2_x86_64.tar.gz -C /home/app &&
        # 去掉v，不显示过程解压过程
        tar -zxf /home/soft/cdh5.16.2/cloudera-manager-centos7-cm5.16.2_x86_64.tar.gz -C /home/app &&
        ln -s /home/app/cm-5.16.2 /opt/cm-5.16.2 &&
        ln -s /home/app/cloudera /opt/cloudera &&

        useradd --system --home=/opt/cm-5.16.2/run/cloudera-scm-server/ --no-create-home --shell=/bin/false --comment "Cloudera SCM User" cloudera-scm

        # vi /opt/cm-5.16.2/etc/cloudera-scm-agent/config.ini
        sed -i "s/server_host=localhost/server_host={master_host}/g" /opt/cm-5.16.2/etc/cloudera-scm-agent/config.ini
        echo "done."
        """.format(master_host=MASTER_HOST)
        self.logger.info(cmd)
        self.agent.run(cmd, encoding="utf-8")
        self.logger.info("cdh agent deploy done.")

    def deploy_cdh_log(self):
        """
        # cdh各组件的用户是何时创建的？只有创建了用户才能执行下面语句
        # 如果重复执行会嵌套一层目录
            ln -s /home/app/cdh-log/hadoop-hdfs /var/log/hadoop-hdfs 
        # 需要修改成如下
            ln -sf /home/app/cdh-log/hadoop-hdfs /var/log 

        mkdir -p /home/app/cdh-log/hadoop-hdfs
        rm -rf /var/log/hadoop-hdfs
        ln -s /home/app/cdh-log/hadoop-hdfs /var/log/hadoop-hdfs
        chown -R hdfs:hadoop /var/log/hadoop-hdfs
        chown -R hdfs:hadoop /home/app/cdh-log/hadoop-hdfs

        mkdir -p /home/app/cdh-log/hadoop-yarn
        rm -rf /var/log/hadoop-yarn
        ln -s /home/app/cdh-log/hadoop-yarn /var/log/hadoop-yarn
        chown -R yarn:hadoop /var/log/hadoop-yarn
        chown -R yarn:hadoop /home/app/cdh-log/hadoop-yarn

        mkdir -p /home/app/cdh-log/hive
        rm -rf /var/log/hive
        ln -s /home/app/cdh-log/hive /var/log/hive
        chown -R hive:hive /var/log/hive
        chown -R hive:hive /home/app/cdh-log/hive

        mkdir -p /home/app/cdh-log/impalad
        rm -rf /var/log/impalad
        ln -s /home/app/cdh-log/impalad /var/log/impalad
        chown -R impala:impala /var/log/impalad
        chown -R impala:impala /home/app/cdh-log/impalad

        mkdir -p /home/app/cdh-log/kudu
        rm -rf /var/log/kudu
        ln -s /home/app/cdh-log/kudu /var/log/kudu
        chown -R kudu:kudu /var/log/kudu
        chown -R kudu:kudu /home/app/cdh-log/kudu

        mkdir -p /home/app/cdh-log/hbase
        rm -rf /var/log/hbase
        ln -s /home/app/cdh-log/hbase /var/log/hbase
        chown -R hbase:hbase /var/log/hbase
        chown -R hbase:hbase /home/app/cdh-log/hbase

        mkdir -p /home/app/cdh-log/zookeeper
        rm -rf /var/log/zookeeper
        ln -s /home/app/cdh-log/zookeeper /var/log/zookeeper
        chown -R zookeeper:zookeeper /var/log/zookeeper
        chown -R zookeeper:zookeeper /home/app/cdh-log/zookeeper

        mkdir -p /home/app/cdh-log/sentry
        rm -rf /var/log/sentry
        ln -s /home/app/cdh-log/sentry /var/log/sentry
        chown -R sentry:sentry /var/log/sentry
        chown -R sentry:sentry /home/app/cdh-log/sentry

        mkdir -p /home/app/cdh-log/oozie
        rm -rf /var/log/oozie
        ln -s /home/app/cdh-log/oozie /var/log/oozie
        chown -R oozie:oozie /var/log/oozie
        chown -R oozie:oozie /home/app/cdh-log/oozie
        """

        user_file_dict = {
            "hdfs:hadoop":["/var/log/hadoop-hdfs","/home/app/cdh-log/hadoop-hdfs"],
            "yarn:hadoop":["/var/log/hadoop-yarn","/home/app/cdh-log/hadoop-yarn"],
            "hive:hive":["/var/log/hive","/home/app/cdh-log/hive"],
            "impala:impala":["/var/log/impalad","/home/app/cdh-log/impalad"],

            "kudu:kudu":["/var/log/kudu","/home/app/cdh-log/kudu"],
            "hbase:hbase":["/var/log/hbase","/home/app/cdh-log/hbase"],
            "zookeeper:zookeeper":["/var/log/zookeeper","/home/app/cdh-log/zookeeper"],
            "sentry:sentry":["/var/log/sentry","/home/app/cdh-log/sentry"],
            "oozie:oozie":["/var/log/oozie","/home/app/cdh-log/oozie"],
        }
        for k, v in user_file_dict.items():
            cmd1 = """
            mkdir -p {trg_path}
            cd /var/log && rm -rf {src_path}
            ln -sf {trg_path} {src_path_previous}
            chown -R {user_group} {src_path}
            chown -R {user_group} {trg_path}""".format(user_group=k, src_path_previous=os.path.dirname(v[0]), src_path=v[0], trg_path=v[1])
            self.logger.info(cmd1)
            # self.agent.run(cmd1, encoding="utf-8")
        self.logger.info("Log folder created.")

    def manage_port(self):
        """使用iptables服务管理端口"""

    def auto_start_cdh(self):
        """增加自启动cdh"""
        file_content = """#!/bin/bash

# auto start cdh

systemctl status iptables
systemctl stop iptables
ntpdate {ntp_host}          # 手动同步下时间，然后再启动服务
service ntpd restart        # 重启ntpd服务
sleep 10s                   # 睡眠10s，否则经常报错ntpdc: read: Connection refused
ntpdc -c loopinfo           # offset为0s即可
# sleep 30s
# /opt/cm-5.16.2/etc/init.d/cloudera-scm-server restart
sleep 30s
/opt/cm-5.16.2/etc/init.d/cloudera-scm-agent restart
swapoff /dev/dm-1           # 关闭交换内存""".format(ntp_host=NTP_HOST)
        cmd = "echo '{}' > /root/auto_start_cdh.sh".format(file_content) 
        self.logger.info(cmd)
        self.agent.run(cmd)

        # 增加自启动
        cmd1 = """
        chmod +x /etc/rc.d/rc.local
        echo "
# auto start cdh
bash /root/auto_start_cdh.sh &>> /root/auto_start_cdh.log
" >> /etc/rc.d/rc.local
        """
        self.logger.info(cmd1)
        self.agent.run(cmd1)
        self.agent.run("bash /root/auto_start_cdh.sh", encoding="utf-8")

    def deploy_tez(self):
        """集成tez计算引擎，需要单独配置，这里主要是文件的分发。/opt/cloudera/parcels/CDH路径只有在cm部署完之后才会生成，所有需要放到后面执行"""
        cmd = f"scp -P{AGENT_PORT} -r /opt/cloudera/parcels/CDH/lib/tez {AGENT_USER}@{AGENT_HOST}:/opt/cloudera/parcels/CDH/lib/"
        self.logger.info(cmd)
        self.server.run(cmd, encoding="utf-8")
        self.logger.info("send tez folder done.")

    def deploy_azkaban(self):
        """部署azkaban"""
        pass

    def add_users(self):
        """
        namenode 节点增加hive权限：
        groupadd developers
        useradd --no-create-home --shell /bin/false -g developers work
        usermod -a -G hive work

        hive client 节点：
        groupadd developers
        useradd -g developers work
        # usermod -a -G hive work  # 这个授权对hive -e没效果，hive -e 必须结合sentry使用
        passwd work
        """


if __name__ == "__main__":
    d = Deploy()
    d.agent.run("ifconfig")
    # d.create_authorized_keys()            # 服务端需要执行
    # d.distribute_authorized_keys()
    # d.distribute_soft()
    # d.add_host()
    # d.install_java()
    # d.set_base_config()
    # d.set_ntp()
    # d.deploy_cdh_agent()
    # d.auto_start_cdh()

    d.logger.info("{} deploy successful.".format(AGENT_HOST))

    # 所有机器部署完毕后最后执行。只有集群新增机器才执行该步骤。
    # d.replace_all_hosts()

    # cm添加新主机到集群中才会创建cdh的所有组件的用户和组，最后再手动执行log目录的映射
    # d.deploy_cdh_log()
    # d.deploy_tez()
