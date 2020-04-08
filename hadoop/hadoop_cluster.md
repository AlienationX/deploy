# CM (CDH)

## 关闭防火墙

```shell
systemctl status iptables
systemctl stop iptables
```

## 免密钥登录

修改主机名，需要重启
vi /etc/hostname
hadoop-prod01
reboot

```shell
ssh-keygen &&
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys &&
chmod 600 ~/.ssh/authorized_keys

key=`cat ~/.ssh/id_rsa.pub`
for i in {01..10}; do
    ip="hadoop-prod"$i
    ssh -p22022 root@$ip <<EOF
echo "$key" >> ~/.ssh/authorized_keys
EOF
    echo "$ip complete"
done

# scp -P22022 ~/.ssh/authorized_keys root@10.63.80.105:~/.ssh/
# scp -P22022 ~/.ssh/authorized_keys root@10.63.80.106:~/.ssh/
# scp -P22022 ~/.ssh/authorized_keys root@10.63.80.107:~/.ssh/
# scp -P22022 ~/.ssh/authorized_keys root@10.63.80.108:~/.ssh/

for i in {105..108}; do
    ip="10.63.80."$i
    key_content=$(cat ~/.ssh/id_rsa.pub)
    # ssh -p22022 root@$ip "echo $key_content >> ~/.ssh/authorized_keys" &&
    ssh -p22022 root@$ip "
    echo '
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

## java

```shell
for i in {105..108}; do
    ip="10.63.80."$i
    scp -P22022 jdk-8u102-linux-x64.tar.gz root@$ip:/home/soft/
    echo "$ip complete"
done

mkdir /opt/freeware &&
tar zxvf /home/soft/jdk-8u102-linux-x64.tar.gz -C /opt/freeware/
echo '
JAVA_HOME="/opt/freeware/jdk1.8.0_102"
PATH=".:$PATH:$JAVA_HOME/bin"
export JAVA_HOME PATH
' >> /etc/profile
source /etc/profile
java -version
```

## mysql

groupadd mysql
useradd -r -g mysql -M mysql

```sql
set password=password('AsiaInfo123!@#');

grant all privileges on `cm`.* to 'hadoop'@'%' identified by 'hadoop';
grant all privileges on `amon`.* to 'hadoop'@'%' identified by 'hadoop';
grant all privileges on `hive`.* to 'hadoop'@'%' identified by 'hadoop';
grant all privileges on `oozie`.* to 'hadoop'@'%' identified by 'hadoop';
grant all privileges on `hue`.* to 'hadoop'@'%' identified by 'hadoop';
flush privileges;
```

```shell
/opt/cm-5.16.2/share/cmf/schema/scm_prepare_database.sh mysql cm -hlocalhost -uroot -p'hadoop' --scm-host localhost scm scm scm

for i in {105..108}; do
    ip="10.63.80."$i
    scp -P22022 cloudera-manager-centos7-cm5.16.2_x86_64.tar.gz ../mysql-connector-java-5.1.48.jar root@$ip:/opt
    echo "$ip complete"
done
```

## 所有节点

> df -h 查看根目录空间大小，如果比较小的话安装目录切记做软链接到home目录下

```shell
useradd --system --home=/opt/cm-5.16.2/run/cloudera-scm-server/ --no-create-home --shell=/bin/false --comment "Cloudera SCM User" cloudera-scm
tar zxvf /home/soft/cloudera-manager-centos7-cm5.16.2_x86_64.tar.gz -C /opt
cp /home/soft/mysql-connector-java-5.1.48.jar /opt/cm-5.16.2/share/cmf/lib/ && ls /opt/cm-5.16.2/share/cmf/lib/ | grep mysql
vi /opt/cm-5.16.2/etc/cloudera-scm-agent/config.ini

# 修改/opt/cm-5.16.2/etc/cloudera-scm-agent/config.ini中的server_host为主节点的主机名。不要用ip

/opt/cm-5.16.2/etc/init.d/cloudera-scm-server start
/opt/cm-5.16.2/etc/init.d/cloudera-scm-agent start
```

## ftp共享安装文件

python -m SimpleHTTPServer 8000

## cdh

echo never > /sys/kernel/mm/transparent_hugepage/defrag

echo never > /sys/kernel/mm/transparent_hugepage/enabled

同时最下面添加

```shell
echo '
# cdh
echo never > /sys/kernel/mm/transparent_hugepage/defrag
echo never > /sys/kernel/mm/transparent_hugepage/enabled
' >> /etc/rc.local
```

## JAVA_HOME not found

```shell
ln -s /opt/freeware/jdk1.8.0_102 /usr/java/jdk1.8.0_102

for i in {105..108}; do
    ip="10.63.80."$i
    # scp -P22022 id_rsa.pub root@$ip:~/
    ssh -p22022 root@$ip "
    mkdir /usr/java
    ln -s /opt/freeware/jdk1.8.0_102 /usr/java/jdk1.8.0_102
    "
    echo "$ip complete"
done
```

## 时区同步问题

```shell
yum install ntp -y
vi /etc/ntp.conf            # server 10.63.90.227
ntpdate 10.63.90.227        # 手动同步下时间，然后再启动服务
service ntpd start
ntpdc -c loopinfo           # offset为0s即可
/opt/cm-5.16.2/etc/init.d/cloudera-scm-agent restart

> 切记一定要重启agent，等待一段时间同步即可，短至5分钟，长至1天不等。
```

## azkaban

```sql
grant all privileges on `azkaban`.* to 'azkaban'@'%' identified by 'azkaban';
source create-all-sql-0.1.0-SNAPSHOT.sql
grant all privileges on `azkaban`.* to 'hadoop'@'%';
```

web-server: 10.63.80.105
exec-server: 10.63.80.107
exec-server: 10.63.80.108

```shell
keytool -keystore keystore -alias jetty -genkey -keyalg RSA
```

https://10.63.80.105:8443

> 路径都是相对路径，所以启动程序时一定要在azkaban根目录启动，切记。 

web-server  azkaban.properties

```txt
# 修改时区

default.timezone.id=Asia/Shanghai

# Azkaban Jetty server properties.

jetty.use.ssl=true  # 修改为true，新增下面几项配置。（启用https协议：http://10.63.80.105:8081 变成了 https://10.63.80.105:8443）
jetty.maxThreads=25
jetty.port=8081

jetty.ssl.port=8443
jetty.keystore=conf/keystore
jetty.password=azkaban
jetty.keypassword=azkaban
jetty.truststore=conf/keystore
jetty.trustpassword=azkaban

# Azkaban mysql settings by default. Users should configure their own username and password.
database.type=mysql
mysql.port=3306
mysql.host=10.63.80.104
mysql.database=azkaban
mysql.user=azkaban
mysql.password=azkaban
mysql.numconnections=100

# mail settings
mail.sender=
mail.host=
```

exec-server  azkaban.properties

```txt
# 修改时区
default.timezone.id=Asia/Shanghai

# Where the Azkaban web server is located
# azkaban.webserver.url=http://localhost:8081
azkaban.webserver.url=https://10.63.80.105:8443

# Azkaban mysql settings by default. Users should configure their own username and password.
database.type=mysql
mysql.port=3306
mysql.host=10.63.80.104
mysql.database=azkaban
mysql.user=azkaban
mysql.password=azkaban
mysql.numconnections=100

# Azkaban Executor settings (增加executor.port，固定端口号)
executor.maxThreads=50
executor.flow.threads=30
executor.port=12321
```

## docker

10.63.80.106
