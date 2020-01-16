# PROD

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

## 步骤

向CDH5集群中添加新的主机节点

步骤一：首先得在新的主机环境中安装JDK，关闭防火墙、修改selinux、NTP时钟与主机同步、修改hosts、与主机配置ssh免密码登录、保证安装好了perl和python.

步骤二：上传cloudera-manager-el6-cm5.0.0_x86_64.tar.gz文件到/opt目录解压，修改agent配置文件：

　　　　vi /opt/cm-5.0.0/etc/cloudera-scm-agent/config.ini

　　　　server_host = Master

步骤三：在该代理节点创建cloudera-scm用户

useradd --system --home=/opt/cm-5.0.0/run/cloudera-scm-server/ --no-create-home --shell=/bin/false --comment "Cloudera SCM User" cloudera-scm

步骤四：启动代理服务,  /opt/cm-5.0.0/etc/init.d/cloudera-scm-agent start

步骤五：在主机节点CM管理页面，进行主机添加，服务添加。

调优： echo 0 > /proc/sys/vm/swappiness

## 基本配置

```shell
ssh-keygen &&
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys &&
chmod 600 ~/.ssh/authorized_keys

for i in 199 200 201 202 203 204 205 191 192; do
    ip="10.63.82."$i
    key_content=$(cat ~/.ssh/id_rsa.pub)
    ssh -p22022 root@$ip "echo $key_content >> ~/.ssh/authorized_keys" &&
    echo "$ip complete"
done


for i in 198 199 200 201 202 203 204 205 191 192; do
    ip="10.63.82."$i
    ssh -p22022 root@$ip "
    echo '
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

10.63.80.104 hadoop-dev01
10.63.80.105 hadoop-dev02
10.63.80.106 hadoop-dev03
10.63.80.107 hadoop-dev04
10.63.80.108 hadoop-dev05
' >> /etc/hosts
    "
    echo "$ip complete"
done
```

## software

```shell
for i in 199 200 201 202 203 204 205 191 192; do
    ip="10.63.82."$i
    scp -P22022 jdk-8u102-linux-x64.tar.gz root@${ip}:/home/soft &&
    echo "$ip complete"
done
```

## java

```shell
yum -y install perl ntp httpd mod_ssl gcc gcc-c++ python-devel libxslt-devel cyrus-sasl* psmisc &&

mkdir /usr/java &&
tar -zxvf /home/soft/jdk-8u102-linux-x64.tar.gz -C /usr/java &&

echo '
JAVA_HOME="/usr/java/jdk1.8.0_102"
PATH=".:$PATH:$JAVA_HOME/bin:"
export JAVA_HOME PATH
' >> /etc/profile &&
source /etc/profile &&
java -version
```

## cm

```shell
mkdir -p /home/app/dfs &&
ln -s /home/app/dfs /dfs &&

mkdir -p /home/app/yarn &&
ln -s /home/app/yarn /yarn &&

tar -zxvf /home/soft/cdh5.16.2/cloudera-manager-centos7-cm5.16.2_x86_64.tar.gz -C /home/app &&
ln -s /home/app/cloudera /opt/cloudera &&
ln -s /home/app/cm-5.16.2 /opt/cm-5.16.2 &&

vi /opt/cm-5.16.2/etc/cloudera-scm-agent/config.ini

useradd --system --home=/opt/cm-5.16.2/run/cloudera-scm-server/ --no-create-home --shell=/bin/false --comment "Cloudera SCM User" cloudera-scm

/opt/cm-5.16.2/etc/init.d/cloudera-scm-server start
/opt/cm-5.16.2/etc/init.d/cloudera-scm-agent start
```

> agent node

```shell
scp -P22022 -r /home/app/cm-5.16.2 root@hadoop-prod09:/home/app/

# scp -P22022 /home/app/cm5162.tar.gz root@hadoop-prod09:/home/soft/

mkdir -p /home/app/dfs &&
ln -s /home/app/dfs /dfs &&

mkdir -p /home/app/yarn &&
ln -s /home/app/yarn /yarn &&

# tar -zxvf /home/soft/cm5162.tar.gz -C /home/app &&
ln -s /home/app/cm-5.16.2 /opt/cm-5.16.2 &&
mkdir -p /home/app/cloudera &&
ln -s /home/app/cloudera /opt/cloudera &&

useradd --system --home=/opt/cm-5.16.2/run/cloudera-scm-server/ --no-create-home --shell=/bin/false --comment "Cloudera SCM User" cloudera-scm

/opt/cm-5.16.2/etc/init.d/cloudera-scm-agent start
```
