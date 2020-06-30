

# mycat 2.0-readme

author:junwen  2020-6-30

作者qq: 294712221

qq群:332702697

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

开发日志: <https://github.com/MyCATApache/Mycat2/blob/master/doc/101-todo-history-list.md>

项目地址:<https://github.com/MyCATApache/Mycat2>

HBTlang文档: <https://github.com/MyCATApache/Mycat2/blob/master/doc/103-HBTlang.md>

Dockerfile:https://github.com/MyCATApache/Mycat2/blob/master/mycat2/Dockerfile

Mycat2可视化监控,使用Grafana和prometheus实现,模板:https://github.com/MyCATApache/Mycat2/blob/master/Mycat2-monitor.json

执行hbt的两组命令是

```sql
EXPLAIN SELECT id FROM db1.travelrecord WHERE id = 1;

EXECUTE plan fromSql(repli,'SELECT `id`  FROM `db1`.`travelrecord`  WHERE `id` = 1')
```

## 安装包下载

https://github.com/MyCATApache/Mycat2/releases




## 正在进行的任务

开发日志: <https://github.com/MyCATApache/Mycat2/blob/master/doc/101-todo-history-list.md>



## 特点

1.proxy透传报文,使用buffer大小与结果集无关

2.proxy透传事务,支持XA事务,jdbc本地事务

3.支持自动,手动分布式查询



## 设计思想

人脑为主,自动化为辅

提倡预规划设计

实用功能为先

禁止潜在风险的调用链



## 相比于1.6

1.6是伪装成数据库的分库分表路由

而2.0是结合手动与自动,伪装成分库分表路由的数据库



## 适用场景

### 精准分片

DML具有明确的分片条件,而且同一个事务内的操作,结合分片算法,总能在同一个分片之内.这是第1大类SQL.

这个场景下,mycat2能提供最佳的执行方式支撑业务

第1大类使用proxy方式处理请求,mycat2自动对sql进行改写,透传响应,客户端操作mycat2几乎与直接操作一个mysql没有区别,事务特性等都没有任何改变

##### 配置关键点

开启proxy事务特性



### 精准分片+分布式查询

在精准分片基础上,有部分DML不具有明确的分片条件,但是对数据一致性没有要求.

这种是第2大类SQL,当分布式查询引擎把查询逻辑表的sql转换成物理表的sql的时候,发现仅仅一个分片就可以完成查询,那么就使用透传处理,此时也能达到与一个mysql的操作特性,如果不能,则使用分布式查询引擎通过jdbc拉取数据.后者可以建立定时预读的缓存结果集,把客户端的多个查询请求转化成对相同的结果集对象查询,大大减少重复的计算量和内存使用.

##### 配置关键点

开启proxy事务特性

使用拦截器配置sql设置缓存



### 分布式事务+分布式查询

在精准分片+分布式查询的基础上开启XA事务,使单一分片事务升级为XA事务,此时事务隔离级别与事务特性受到XA特性约束,mycat基本上会把所有请求都转化成jdbc接口的操作,不再使用透传.可结合结果集缓存特性提高查询性能

##### 配置关键点

开启xa事务特性



### 读写分离

见往下的配置



### 基于sql后端的大数据查询工具

mycat2支持HBT语言方式向后端数据库发送sql拉取数据,然后使用特定语法聚合结果,并建立缓存



### 嵌入式数据库客户端接口(正在完善)

mycat2支持不启动网络层的方式,以api方式操作mycat,实现执行sql



## 开发环境
参考src\main\resources\sql中的sql和src\main\resources\mycat.yml建立数据库环境
ide安装lombok插件
启动 io.mycat.MycatCore类



如果遇上在maven模块之间出现版本引用错误,可以使用下面描述的设置版本重置所有模块的版本号



## 编译环境

编译环境要与生产环境的jdk对应

例如JDK8编译的安装包不能在JDK9下运行,否则可能出现运行失败



## 打包

```shell
mycat模块下

mvn package
mvn package -Dmaven.test.skip=false
mvn package -Dmaven.test.skip=true
```





## 设置版本

```
versions:set -DnewVersion=1.xxx-SNAPSHOT
```



## 安装执行

db1.sql,db2.sql,db3.sql是测试用的表



#### 安装包执行

出现权限不足请提升到管理员权限



###### linux

```shell
下载安装包
wget http://dl.mycat.org.cn/2.0/xxxx
tar -xvf xxx.gz
修改/root/mycat/conf/mycat.yml文件
cd mycat/bin
./mycat start
./mycat status
```



```shell
./mycat start 启动
./mycat stop 停止
./mycat console 前台运行
./mycat install 添加到系统自动启动（暂未实现）
./mycat remove 取消随系统自动启动（暂未实现）
./mycat restart 重启服务
./mycat pause 暂停
./mycat status 查看启动状态
```



###### windows

```shell
下载安装包
http://dl.mycat.org.cn/2.0/xxxx
tar -xvf xxx.gz
修改/root/mycat/conf/mycat.yml文件
cd mycat/bin
./mycat insatll
./mycat start
./mycat status
```



#### jar包执行

```shell

java -Dfile.encoding=UTF-8 -DMYCAT_HOME=mycat2\src\main\resources  -jar mycat2-0.5-SNAPSHOP.jar

Mycat2\mycat2\src\main\resources 是mycat.yml所在的文件夹
配置加载可以通过替换
io.mycat.ConfigProvider实现不同的配置加载方式
```



#### Mycat连接测试



##### 客户端连接mycat

测试mycat与测试mysql完全一致，mysql怎么连接，mycat就怎么连接。

在mysqld下面设置

default_authentication_plugin = mysql_native_password

客户端登录参数

--default-auth-password=mysql_native_password

--default-auth=mysql_native_password

推荐先采用命令行测试：

```
mysql -uroot -proot -P8066 -h127.0.0.1
```



mysql8客户端要加上-A参数禁用预读功能

```
mysql -A -uroot -proot -P8066 -h127.0.0.1
```



客户端登录记录

LINUX平台客户端

```bash
mysql  Ver 15.1 Distrib 10.1.44-MariaDB, for debian-linux-gnu (x86_64) using rea
```



```
mysql  Ver 14.14 Distrib 5.6.33, for debian-linux-gnu (x86_64) using  EditLine wrapper
```



WINDOWS平台客户端

```
mysql  Ver 15.1 Distrib 10.3.15-MariaDB, for Win64 (AMD64), source revision 07aef9f7eb936de2b277f8ae209a1fd72510c011
```



```
mysql  Ver 8.0.19 for Win64 on x86_64 (MySQL Community Server - GPL)
```



```
SQLyog XXXX - MySQL GUI v12.3.1(64 bit)
```



```
Navicat xxxx 12.1.22(64 bit)
```



```
MySQL Workbench 8.0.19
```

支持select  current_user()



##### 客户端要求

关闭SSL

启用客户端预处理,关闭服务器预处理

mysql_native_password授权

开启自动重连

开启闲置连接检查,心跳

```
com.mysql.jdbc.exceptions.jdbc4.CommunicationsException: Communications link failure

Can not read response from server. Expected to read 4 bytes, read 0 bytes before connection was unexpectedly lost.
```

关闭允许多语句

jdbc客户端设置useLocalSessionState解决

```
Could not retrieve transation read-only status server
```



##### Mycat连接MySql

Mycat连接不上Mysql的问题

ip配置错误,无法连通,例如本地ip

0.0.0.0 

localhost 

127.0.0.1

没有权限可能出现连接不上的现象



##### 连接状态问题

数据源的initSqls属性可以设置连接初始化的变量

如果mysql的编码是utf8mb4,那么请写上

```
set names utf8mb4;
```

如果要初始化默认库,请写上

```
use db1;
```

jdbc的连接属性建议使用连接字符串设置

如果使用图形化客户端出现*no* *database* *selected* 等提示,请在JDBC连接字符串上写上默认库



##### mysql服务器设置参考

###### MariaDB 10.3

```ini
[mysqld]
local-infile=1
local-infile = ON
datadir=xxx/MariaDB 10.3/data
port=3306
innodb_buffer_pool_size=2031M
max_allowed_packet=128MB
max_connections=10000
character-setVariable-client-handshake = FALSE 
character-setVariable-server = utf8mb4 
collation-server = utf8mb4_unicode_ci 
init_connect='SET NAMES utf8mb4'
log_bin_trust_function_creators=1
[client]
local-infile = ON
loose-local-infile= 1
port=3306
plugin-dir=xxx/MariaDB 10.3/lib/plugin
default-character-setVariable = utf8mb4
[mysql] 
local_infile = 1
local-infile = ON
default-character-setVariable = utf8mb4

```



###### Mysql-8.0.19

```ini
[mysqld]
port=3307
basedir=xx/mysql-8.0.19-winx64/mysql-8.0.19-winx64
# 设置mysql数据库的数据的存放目录
datadir=xx/mysql-8.0.19-winx64/mysql-8.0.19-winx64/Database
max_connections=200
max_connect_errors=10
character-setVariable-server=utf8mb4
default-storage-engine=INNODB

#mycat2.0可能不支持其他授权方式
default_authentication_plugin=mysql_native_password 
[mysql]
# 设置mysql客户端默认字符集
default-character-setVariable=utf8mb4

....
```



#### 日志配置



JVM调试参数

```bash
-Dlog4j.debug
```



wrapper.conf

```ini
wrapper.java.additional.10=-Dlog4j.configuration=file:/root/mycat/conf/log4j.properties
```



log4j.properties

```ini
log4j.rootLogger=debug,console,rollingFile
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{HH:mm:ss} T=%t [%c %M at %L]-[%p] %m%n


log4j.appender.rollingFile=org.apache.log4j.rolling.RollingFileAppender
log4j.appender.rollingFile.RollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy
#此处修改日志路径
log4j.appender.rollingFile.RollingPolicy.ActiveFileName=../logs/mycat.log
log4j.appender.rollingFile.RollingPolicy.FileNamePattern=../logs/mycat-%d{yyyy-MM-dd}.%i.log.gz
log4j.appender.rollingFile.triggeringPolicy=org.apache.log4j.rolling.SizeBasedTriggeringPolicy
#100MB
log4j.appender.rollingFile.triggeringPolicy.MaxFileSize=104857600 
log4j.appender.rollingFile.layout=org.apache.log4j.PatternLayout
log4j.appender.rollingFile.layout.ConversionPattern=%d{HH:mm:ss} T=%t [%c %M at %L]-[%p] %m%n


```



## Mycat2流程

客户端发送SQL到mycat2,mycat2拦截对应的SQL执行不同的命令,对于不需要拦截处理的SQL,透传到有逻辑表的mysql,这样,mycat2对外就伪装成mysql数据库



## 概念

### 物理库

存储节点上的库



### 物理表

存储节点上的表



### 逻辑库

逻辑表组成的集合



### 逻辑表

mycat进行处理的表,mycat会把逻辑表的操作翻译成物理库的操作,并执行



逻辑库/逻辑表与物理库/表是相对的,mycat接收的sql中使用的库,表就是逻辑库,逻辑表.而Mycat发往后端数据库的库名,表名就是物理库,物理表



## MetaData配置

### 概念

#### 存储节点


dataNode是数据节点,库名,表名组成的三元组

targetName是目标名字,它可以是数据源的名字或者集群的名字



#### 分片范围

​		如果SQL条件是有完全独立(隔离性)的分片逻辑,就结合条件与数据的访问路径(schema.database.table)进行拆分.

​		实例控制进程资源,实例上的一个连接是事务操作的基本单位,如果单实例整体无法满足数据的增长,就拆分实例.一般来说,如果在实例分片上使用枚举分片,随着数据的增长,实例的数据也是继续增长.如果要保证实例存储的数据量不再增长就要继续拆分实例.因此,建议实例分片使用方便扩容的算法,比如一致性哈希.另一方面,拆分实例会引入分布式事务问题,涉及多实例的更新操作,会比单实例慢.涉及多个实例的SQL产生额外的耗时不可避免.

​		一个物理表对应一个存储结构(锁与索引),如果存储结构无法满足数据存储,就拆分存储结构.如果查询中的分片条件生效,则将会减少操作计算的存储空间.一个存储结构可能对应一个IO句柄,如果分片条件不生效,导致所有分片都扫描,浪费IO句柄.建议分表通过预先发现单表的索引算法上瓶颈与单实例硬件瓶颈的方法.准确划分一个分片数量.即单库分表再也无法提高查询性能的分片数量,此时运算数据的耗时与拆分存储结构得省时抵消.

​		表属性与SQL优化有关.数据分片查询引擎在单实例上执行的SQL要根据实际的表属性的约束进行优化.如果表配置了分区表,影响的因素还会更多.分区表作为一种数据库提供的分片技术,它的利弊与分片分库分表很类似.

​		不同的分片字段可能有着不同的分片范围,这些范围之间可能是有交集的,多个SQL条件计算出范围,得出具有最小范围的等价字段,使用该范围进行查询.例如如果分片字段直接就能计算出物理表的访问范围,则使用该路径访问.如果不能.则结合跨库与辅助的分片范围缩小查询物理表的范围.



分片必然分库,分库必然分表


| 目标 targetName | 库schemaName                 | 表tableName                | 类型             |
| --------------- | ---------------------------- | -------------------------- | ---------------- |
| 唯一            | 唯一                         | 唯一                       | 非分片           |
| 唯一            | 多目标                       | 相同名字                   | 非分片分库分表   |
| 唯一            | 唯一                         | 不同名字                   | 非分片单库分表   |
| 跨实例          | 多目标                       | 不同名字                   | 分片分库分表     |
| 适合扩容的规则  | 多个字段多种规则缩小查询范围 | 单实例最大数据量适配的规则 | 分片分库分表索引 |



#### 建立物理库,物理表的规律



##### 规律1:与逻辑库名称路径一致的物理表

用于

1. schema上的配置默认目标
2. 在代理架构下即mycat proxy没有设置成自动返回show语句的情况(比如使用命令配置),则配置的默认命令指向的默认目标



此默认目标应该有非分片表与分片表的名字的物理库,此物理库具有提供查询逻辑表信息的作用(因为表名,库名与逻辑表一致),无需经过sql改写即可查询逻辑表的数据



##### 规律2:若有全局表,则每一个跨服务器的分片建议建立一个相同名称路径的物理表

这样的好处是涉及分片表与全局表的sql无需复杂分析,若判断sql涉及一个分片的分片表和任意多个全局表,只需改写分片表部分即可查询







####   Mycat2的分片分库分表运算

​		在分片分库分表中运算分为两个部分,一部分是后端每个数据库的运算,这部分运算以SQL作为中间语言发送到后端服务器,一部分以HBT形式在mycat里执行,占用内存主要是驻留的结果集的总大小.如果结果集合拼的结果行是固定行,固定列,结果集每个值长度也是固定的,那意味着运算都是reduce的,可以边运算边丢弃已处理的值,无需保存完整的后端处理结果.

​		当mycat2无法下推大部分运算的时候(主要是join,后面会继续优化),则可能拉取大结果集,处理还是很耗时的.所以尽量使用分片谓词靠近数据源风格编写SQL,便于mycat2识别可下推的谓词.



#### 事务类型

##### 透传式单分片事务,即代理事务

前端会话只能绑定一个后端数据库会话

后端会话的响应直接往前端会话写入数据,自动判断响应结束以及事务



问题场景

travelrecord具有全局序列号id字段,而且id是hash型的分片算法

```sql
//开始事务
INSERT INTO `db1`.`travelrecord` (`user_id`) VALUES ('2');
//产生id 998,分片算法得出该sql路由到数据源1并开启该后端会话的事务,前端会话绑定该会话直到事务消失
INSERT INTO `db1`.`travelrecord` (`user_id`) VALUES ('2');
//产生id 999,分片算法得出该sql路由到数据源2,但是已经绑定数据源1的会话了,所以插入失败
```



该事务类型具有极佳的响应性能,极少内存占用,但是要求业务代码在事务中操作数据的时候要准确操作一个分片的数据

一般来说,解决这个问题有五个办法

1. 使用能够直接从分片字段值就可以看出是否在操作单分片的分片算法(枚举,日期)能够直接避免这个问题
2. 使用单分片下的分库分表/分库/分表
3. 同一个事物中的语句,使用动态分片或者注解指定目标总是在同一个分片
4. 使用XA事务
5. 不开启事务



##### XA事务

https://www.atomikos.com/



##### 其他支持

Mycat2提供的事务执行环境

一个事务绑定一个线程

一个事务根据标记绑定线程

支持以便方便支持不同的事务框架



#### 读写分离

```yaml
metadata: 
  schemas: [{
              schemaName: 'db1' ,   targetName: 'repli',
            }]
```

mycatdb命令可以自动完成sql分析,进行读写分离,对于sql中没有库名的表,会自动添上逻辑库名字

targetName可以是数据源或者集群



#### 分片类型

##### 自然分片

单列或者多列的值映射单值,分片算法使用该值计算数据节点范围

##### 全局表

该表通过冗余存储数据到分片节点上,使用分片查询能与该表的运算能在同一个分片节点上执行,这部分运算不再需要Mycat进行数据聚合

##### 动态分片(高级功能,一般不使用)

单列或者多列的值在分片算法计算下映射分片目标,目标库,目标表,得到有效的数据节点范围

```yaml
select * from db1.address1 where userid = 1 and  id = '122' and addressname ='abc';
```

即根据userid,id,addressname得出分片目标,目标库,目标表

例子1:id ->集群名字;id->库名;id->表名

例子2:任意值->固定集群名字;address_name->库名;user_name->表名

例子3:任意值->固定集群名字,固定库名;address_name->表名;user_name->表名

总之能通过条件中存在的字段名得出存储节点三元组即可

多列值映射到一个值暂时不支持,后续支持



##### 配置

###### 存储节点元素配置

dataNodes

```yaml
dataNodes: [{targetName: defaultDs2 ,schemaName: db1, tableName: company},...]
```

targetName是目标名字,可以是集群名字或者数据源名字

在同一个dataNodes里面的targetName要么全是集群名字要么全是数据源名字,不能出现混合的情况.

schemaName:物理库名

tableName:物理表名

从配置可知,mycat2.0本质上不区分,分库还是分表,仅仅要得知数据从哪里读取,修改



###### 基本配置模板

```yaml
metadata: 
  prototype: {targetName: defaultDs }
  schemas: [{
              schemaName: 'db1' ,   targetName: 'repli', #读写分离集群
              shadingTables: {
                travelrecord: { #表配置
                }
              },
              globalTables: { #表配置
                company: {

                }
              }
            }]
```



prototype: {targetName: defaultDs } 

可不配置

 配置全局默认的数据源,一般可能是无表sql,或者从此处获取表的配置,该数据源配置的意义是代理架构下,后端可能需要存在一个与mycat相同库,相同表的数据源处理mycat无法处理的sql



schemaName

库名



targetName

可以是数据源或者集群名字

如果不配置库,表的信息,无法路由的表发往该目标



###### 全局表配置

该配置也可以作为普通表,非分片表配置

```yml
      globalTables:
        company:
          createTableSQL: #创建表的sql
          dataNodes: [{targetName: defaultDs2 ,schemaName: db1, tableName: company},...]
```



###### 分片表配置

共同点:

1.都需要建表sql



此sql在mycat v1.09之后会自动从dataNode中查询得到,无需配置,但是遇上得到的createTableSQL是mycat无法解析的时候,就需要在mycat里面调整此sql直到mycat的sql解析器能识别并设置



2.当建表sql中的字段信息带有AUTO_INCREMENT同时配置中有配置全局序列号,则该sql在插入数据的时候,自动改写sql补上自增值



3.要么是自然分片要么是动态分片

mycat v1.09后当分片类型不配置的时候,默认是NATURE_DATABASE_TABLE



###### 建表sql的作用

1.主要是为分布式查询引擎提供逻辑表与物理表的字段信息,具体体现为mycat能对查询逻辑表的sql编译成查询物理表的sql,如果没有字段信息,mycat就不能准确生成查询的字段.在配置中都提供字段信息的情况下,mycat可以脱离后端数据库独立编译sql为执行计划.

2.提供自增字段的信息,是开启全局序列号的前提

3.当建表sql带有索引信息,主键信息的时候,分布式查询引擎可以利用它优化算子(mycat v1.09)

4.mycat会把分片字段与主键自动认为是带有索引的字段(mycat v1.09)



###### 自然分片表配置

```yml
        travelrecord: #逻辑表名
          columns:
            - columnName: id #分片字段信息,显式提供,
              shardingType: NATURE_DATABASE_TABLE
              function: #分片算法信息
          createTableSQL: #建表sql
          dataNodes:  #存储节点
```



样例

```yaml
travelrecord: #逻辑表名
  columns:
    - columnName: id #分片字段信息,显式提供,
      shardingType: NATURE_DATABASE_TABLE #类型:自然分片,即根据一列(支持)或者多个列(暂不支持)的值映射成一个值,再根据该值通过单维度的分片算法计算出数据分片范围
      function: { clazz: io.mycat.router.function.PartitionByLong , name: partitionByLong, properties: {partitionCount: '4', partitionLength: '256'}, ranges: {}}
      #提供表的字段信息,升级计划:通过已有数据库拉取该信息
  createTableSQL: |-
    CREATE TABLE `travelrecord` ( `id` bigint(20) NOT NULL AUTO_INCREMENT,`user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,`traveldate` date DEFAULT NULL,`fee` decimal(10,0) DEFAULT NULL,`days` int(11) DEFAULT NULL,`blob` longblob DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  dataNodes: [{targetName: defaultDs ,schemaName: db1, tableName: travelrecord},
              {targetName: defaultDs ,schemaName: db1, tableName: travelrecord2},
              {targetName: defaultDs ,schemaName: db1, tableName: travelrecord3},
              {targetName: defaultDs2 ,schemaName: db2, tableName: travelrecord}] #9999999999
```



###### 动态分片表配置

```yaml
address:
  columns:
    - columnName: id
      shardingType: MAP_TARGET #映射目标
      function: #分片算法
      map: [defaultDs,....] #分片算法得出的值作为下标对应的信息
    - columnName: addressname
      shardingType: MAP_SCHMEA #映射库
      function: #分片算法
      map: [db1,...] #分片算法得出的值作为下标对应的信息
    - columnName: addressname
      shardingType: MAP_TABLE #映射表
      function: #分片算法
      map: [address,address1,....] #分片算法得出的值作为下标对应的信息
  createTableSQL: CREATE TABLE `address1` (`id` int(11) NOT NULL,`addressname` varchar(20) DEFAULT NULL,PRIMARY KEY (`id`))
  dataNodes: [{targetName: defaultDs ,schemaName: db1, tableName: address}]
```

shardingType类型MAP_TARGET,MAP_DATABASE,MAP_TABLE和map必须一起配置

当分片算法根据分片值计算出0的时候,也就是指向map中第一个元素,当分片算法根据分片值计算出1的时候,也就是指向map中第二个元素



样例

```yaml
address:
  columns:
    - columnName: id
      shardingType: MAP_TARGET #动态分片类型,通过任意值映射分片目标,目标库,目标表,要求查询条件要求包列信息,否则可能路由失败,如果配置了dataNode,则会使用dataNode校验
      function: { clazz: io.mycat.router.function.PartitionConstant , properties: {defaultNode: '0'}} #映射到第一个元素
      map: [defaultDs]
    - columnName: addressname
      shardingType: MAP_SCHMEA
      function: { clazz: io.mycat.router.function.PartitionConstant , properties: {defaultNode: '0'}}
      map: [db1]
    - columnName: addressname
      shardingType: MAP_TABLE
      function: { clazz: io.mycat.router.function.PartitionConstant , properties: {defaultNode: '0'}}
      map: [address]
  createTableSQL: CREATE TABLE `address1` (`id` int(11) NOT NULL,`addressname` varchar(20) DEFAULT NULL,PRIMARY KEY (`id`))
  dataNodes: [{targetName: defaultDs ,schemaName: db1, tableName: address}]
```



## 拦截器配置

### 拦截器

由用户登录信息和SQL匹配器两个部分组成

#### 用户登录信息

用户名:区分大小写

密码:区分大小写,不写密码就是忽略密码

ip:用户连接的远程ip接收的格式是

```
127.0.0.1:8888
```

支持正则表达式匹配



###### 默认配置

```yaml
interceptors:
  [{
     user: {ip: '.', password: '123456', username: root},
     transactionType: proxy
   }]
```

此配置使用内置默认的mycatdb命令,根据分片配置进行处理,无需配置任何命令,默认事务是proxy

该属性在mycat连接初始化的时候设置上,但是可以通过sql改变连接中的事务模式



可选的事务

proxy

xa



设置proxy是使用基于一个连接上实现的本地事务

设置xa实际上是指使用数据源提供者的事务实现



当数据源提供者为

```yaml
datasourceProviderClass: io.mycat.datasource.jdbc.datasourceProvider.AtomikosDatasourceProvider
```



当前事务状态为xa,则使用Atomikos的xa事务,并占用bindTransactionPool中的线程

而提供者是其他类的时候,则不会使用bindTransactionPool中的线程

直接使用 workerPool中的线程



例如

```yaml
datasourceProviderClass: io.mycat.datasource.jdbc.datasourceProvider.DruidDatasourceProvider
```

当使用此数据源提供者的时候使用workerPool线程池,当设置xa事务的时候,是使用此DruidDatasourceProvider实现的事务是本地事务,多个连接commit在阶段失败,已经commit的连接不能回滚



#### booster架构

```yaml
interceptors:
  [{
     user: {ip: '.', password: '123456', username: root},
     boosters: [defaultDs2],
     sqls:[
    
     ]
   }]
```

例子：

https://github.com/MyCATApache/Mycat2/blob/master/example/src/test/resources/io/mycat/example/booster/mycat.yml

```yaml
boosters: [defaultDs2]
```

在无事务且开启自动提交的情况下指定的sql发送到后端目标,该属性被mycatdb和boostMycatdb两个命令使用

mycatdb会自动给缺默认库名的sql添上逻辑库,boostMycatdb则不会.

此转发的后端目标一般是高性能的查询服务,mycat在此仅仅是高性能,处理事务或更新请求,对于查询,转发到其他服务处理



SQL匹配,命令是高级内容,见后几章

## 事务

XA事务使用基于JDBC数据源实现,具体请参考Java Transaction API

Proxy事务即通过Proxy操作MySQL进行事务操作,本质上与直接操作MySQL没有差异.

为了方便上层逻辑操作事务,所以统一JDBC和Proxy操作,,参考JDBC的接口,定下mycat2的事务接口
在proxy事务下,开启自动提交,没有事务,遇上需要跨分片的非查询操作,会自动升级为通过jdbc操作



## 数据源配置

```yaml
datasource:
  datasources: [{name: defaultDs, ip: 0.0.0.0,port: 3306,user: root,password: 123456,maxCon: 10000,minCon: 0,
   maxRetryCount: 1000000000, #连接重试次数
   maxConnectTimeout: 1000000000, #连接超时时间
   dbType: mysql, #
   url: 'jdbc:mysql://127.0.0.1:3306?useUnicode=true&serverTimezone=UTC',
   weight: 1, #负载均衡权重
   initSqls: ['use db1'], #建立连接后执行的sql,在此可以写上use xxx初始化默认database,该配置可能无效
   instanceType: ,#READ,WRITE,READ_WRITE ,集群信息中是主节点,则默认为读写,副本则为读,此属性可以强制指定可写
   initSqlsGetConnection: false
  }
  ]
  datasourceProviderClass: io.mycat.datasource.jdbc.datasourceProvider.AtomikosDatasourceProvider
  timer: {initialDelay: 1000, period: 5, timeUnit: SECONDS}
```



### maxConnectTimeout

单位millis

配置中的定时器主要作用是定时检查闲置连接



### initSqlsGetConnection

true|false

默认:false

对于jdbc每次获取连接是否都执行initSqls



### datasourceProviderClass

数据源提供者

涉及jdbc,xa需要特定配置的DataSource,可以实现这个类,暂时mycat只支持mysql的数据源配置,使用mysql的xa数据源



### type

数据源类型

###### NATIVE

只使用NATIVE协议(即Mycat自研的连接MySQL的协议)

###### JDBC

只使用JDBC驱动连接

示例

https://github.com/MyCATApache/Mycat2/blob/master/example/src/test/resources/io/mycat/example/sharingXA/mycat.yml



###### NATIVE_JDBC

默认配置

该数据源同一个配置同时可以使用NATIVE,JDBC





## 集群配置

```yaml
cluster: #集群,数据源选择器,既可以mycat自行检查数据源可用也可以通过mycat提供的外部接口设置设置数据源可用信息影响如何使用数据源
  close: true #关闭集群心跳,此时集群认为所有数据源都是可用的,可以通过mycat提供的外部接口设置数据源可用信息达到相同效果
  clusters: [
  {name: repli ,
   replicaType: SINGLE_NODE , # SINGLE_NODE:单一节点 ,MASTER_SLAVE:普通主从 GARELA_CLUSTER:garela cluster
   switchType: NOT_SWITCH , #NOT_SWITCH:不进行主从切换,SWITCH:进行主从切换
   readBalanceType: BALANCE_ALL  , #对于查询请求的负载均衡类型
   readBalanceName: BalanceRoundRobin , #对于查询请求的负载均衡类型
   writeBalanceName: BalanceRoundRobin ,  #对于修改请求的负载均衡类型
   masters:[defaultDs], #主节点列表
   replicas:[defaultDs2],#从节点列表
   heartbeat:{maxRetry: 3, #心跳重试次数
              minSwitchTimeInterval: 12000 , #最小主从切换间隔
              heartbeatTimeout: 12000 , #心跳超时值,毫秒
              slaveThreshold: 0 , # mysql binlog延迟值
              requestType: 'mysql' #进行心跳的方式,mysql或者jdbc两种
   }}
  ]
  timer: {initialDelay: 1000, period: 5, timeUnit: SECONDS} #心跳定时器
```

只有MASTER_SLAVE,GARELA_CLUSTER能在masters属性配置多个数据源的名字

MASTER_SLAVE中的masters的意思是主从切换顺序

GARELA_CLUSTER的masters意思是这些节点同时成为主节点,负载均衡算法可以选择主节点

requestType是进行心跳的实现方式,使用mysql意味着使用proxy方式进行,能异步地进行心跳,而jdbc方式会占用线程池

当配置是主从的时候,发生主从切换,mycat会备份原来的配置(文件名带有版本号)然后使用更新的配置



## 服务器配置

基础配置样例

```yaml
server:
  ip: 0.0.0.0
  port: 8066
  reactorNumber: 1
```



```yml
  #用于多线程任务的线程池,v1.09前的配置
  worker: {
           maxPengdingLimit: 65535, #每个线程处理任务队列的最大长度
           maxThread: 1024,
           minThread: 2,
           timeUnit: SECONDS, #超时单位
           waitTaskTimeout: 30 #超时后将结束闲置的线程
  }
```



v1.09后把原线程池划分为三大类



bindTransactionPool

对于Atomikos这种对于事务运行环境有要求的事务框架,它要求事务与线程相关,当使用事务的会话与线程绑定之后,在事务消失之前,此线程都不能被其他需要使用事务的会话使用.对于这种特殊要求的事务框架,使用独立的线程池处理事务请求.



workerPool

对于一些耗时长的,可能涉及阻塞的任务,jdbc请求,事务与线程没有绑定关系的事务处理,在这个线程里处理

如Druid数据源提供的本地事务处理,并行拉取结果集等任务,就是这个线程里面处理的.



timeWorkerPool

对于对时间周期敏感的任务,使用独立的定时器处理,但是此定时器一般处理线程比较少,不会处理耗时任务,往往把任务投递到workerPool中处理



三个线程池的配置都是一致的

```yml
 {corePoolSize: 0, keepAliveTime: 1, maxPendingLimit: 65535,
    maxPoolSize: 512, taskTimeout: 1, timeUnit: MINUTES}
```

corePoolSize:是线程池里保留的最小线程数量

keepAliveTime:线程存活时间,超过此时间的空闲线程将会关闭

maxPoolSize:线程池中最大线程数量

timeUnit:时间单位,对keepAliveTime,taskTimeout生效

一般来说,taskTimeout与maxPendingLimit仅仅对bindTransactionPool生效



```yml
server:
  bindTransactionPool: {corePoolSize: 0, keepAliveTime: 1, maxPendingLimit: 65535,
    maxPoolSize: 512, taskTimeout: 1, timeUnit: MINUTES}
  bufferPool:
    args: {}
    poolName: null
  handlerName: null
  ip: 0.0.0.0
  port: 8066
  reactorNumber: 1
  timeWorkerPool: {corePoolSize: 0, keepAliveTime: 1, maxPendingLimit: 65535, maxPoolSize: 2,
    taskTimeout: 1, timeUnit: MINUTES}
  timer: {initialDelay: 3, period: 15, timeUnit: SECONDS}
  workerPool: {corePoolSize: 8, keepAliveTime: 1, maxPendingLimit: 65535, maxPoolSize: 1024,
    taskTimeout: 1, timeUnit: MINUTES}
```



## 分片算法配置

```yaml
function: { clazz: io.mycat.router.function.PartitionByLong , name: partitionByLong, properties: {partitionCount: '4', partitionLength: '256'}, ranges: {}}
```

具体参考以下链接

https://github.com/MyCATApache/Mycat2/blob/master/doc/17-partitioning-algorithm.md



## 负载均衡配置

```yaml
plug:
  loadBalance:
    defaultLoadBalance: balanceRandom
    loadBalances: [
    {name: BalanceRunOnMaster, clazz: io.mycat.plug.loadBalance.BalanceRunOnMaster},
    {name: BalanceLeastActive, clazz: io.mycat.plug.loadBalance.BalanceLeastActive},
    {name: BalanceRoundRobin, clazz: io.mycat.plug.loadBalance.BalanceRoundRobin},
    {name: BalanceRunOnMaster, clazz: io.mycat.plug.loadBalance.BalanceRunOnMaster},
    {name: BalanceRunOnRandomMaster, clazz: io.mycat.plug.loadBalance.BalanceRunOnRandomMaster}
    ]
```

具体参考以下链接

https://github.com/MyCATApache/Mycat2/blob/master/doc/16-load-balancing-algorithm.md



## 全局序列号

```yaml
plug:
  sequence:
    sequences: [
    {name: 'db1_travelrecord', clazz: io.mycat.plug.sequence.SequenceMySQLGenerator ,args: "sql : SELECT db1.mycat_seq_nextval('GLOBAL') , targetName:defaultDs"},
    {name: 'db1_address', clazz: io.mycat.plug.sequence.SequenceSnowflakeGenerator ,args: 'workerId:1'},
    ]
```

名称约定

db1_travelrecord对应metaData配置中的db1.travelrecord,当该名字对应,建表的自增信息存在的时候,自增序列就会自动开启

distributedQuery命令可以查询对应序列号

```sql
SELECT next_value_for('db1_travelrecord')
```



### io.mycat.plug.sequence.SequenceMySQLGenerator

对应1.6 mysql数据库形式的全局序列号

sql是最终查询数据库的sql

targetName是数据源的名字

所需的函数脚本

https://github.com/MyCATApache/Mycat2/blob/052973dfd0a9bd1b1bce85190fd5e828bb9c6a12/mycat2/src/main/resources/dbseq.sql

### io.mycat.plug.sequence.SequenceSnowflakeGenerator

workerId对应雪花算法的参数



## Mycat2.0分布式查询支持语法

select语法



```yaml
query:

select:
      SELECT [ STREAM ] [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]

selectWithoutFrom:
      SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ ( LEFT | RIGHT | FULL ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

joinCondition:
      ON booleanExpression
  |   USING '(' column [, column ]* ')'

tableReference:
      tablePrimary
      [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'

columnDecl:
      column type [ NOT NULL ]

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'

```

Select 语法支持使用for update结尾表示在事务中涉及的查询行使用排它锁,mycat会在最终发送的sql语句中加上for update后缀



UNION语法(v1.09)

```
SELECT UNION [ALL | DISTINCT] SELECT ...
```

UNION语法中不支持使用for update,如果sql中添加了for update则自动忽略



## Mycat2.0分布式修改支持语法

暂时仅仅改写对应的表名和根据条件拆分sql,具体使用explain语句查看



## HBT(Human Brain Tech)

HBT在Mycat2中表现为关系表达式领域驱动语言(Relation DSL).

在设计上是Mycat2的运行时的中间语言,关于查询的HBT可以实现与SQL,其他查询优化器,查询编译器的关系表达式相互甚至与SQL DSL框架中的DSL转换.HBT也支持直接从文本和编写代码的方式构建.



## 使用HBT解决什么问题?

1.允许用户直接编写关系表达式实现功能,不同的SQL方言可以对应同一套关系表达式

2.运行用户运行自定义函数

3.免去优化过程,用户编写的关系表达式可以就是最终的执行计划

4.允许使用函数宏展开关系表达式,比如给逻辑表函数宏指定分片范围自动扩展成对应的物理表

5.允许SQL与关系表达式一并编写,例如叶子是SQL,根是Mycat的运算节点

6.可移植到其他平台运行

7.使用DSL作为中间语言下发到其他Mycat节点上运行

8.方便验证测试

HBTlang文档: <https://github.com/MyCATApache/Mycat2/blob/master/doc/103-HBTlang.md>



## 已知限制问题

###### 不支持服务器预处理

###### proxy事务模式

开启事务后的操作只能是同一个分片

事务里使用全局表会出现非同一分片的全局表无法回滚的现象

对于这种更新操作,,要求强一致性,可以开启xa



###### 分布式查询引擎

1. 结果集字段名不写别名的情况下,生成的列名是不确定的

2. sql不写order的情况下,结果集可能是未经排序的

3. 不建议写类似sql,sql中没有引用表的列名,这种sql在mycat里未正式支持(0.8版本后可以运行)

   `select 1 from db1.travelrecord where id = 1 limit 1`

4. sql一般带有分片条件,否则无法发挥分表优势,而且位于表名后的where,而且是简单的形式,复杂的条件和不写条件,not表达式都会导致全表扫描

5. sql函数名不能出现Crudate大小写混合的情况,否则无法识别

6. 不建议使用除法,不同的数据库的除法的结果存在整形和浮点两种,使用除法请在sql中使用cast保证类型和类型或者*1.0

7. avg函数默认结果是取整的,所以参数值用*1.0转成浮点可以保证精度

8. 聚合函数max,min函数不能与group by一起用

10. 非查询语句,mycat暂时不会自动处理函数表达式调用,会路由到mysql中调用,所以按日期分表的情况,需要sql中写清楚日期

11. 部分关联子查询暂时不支持(不可下推的子查询)



分布式查询引擎(calcite)检查项

in表达式会编译成多个or表达式,默认情况下会把超过20个常量值变成内联表,mycat2要对此不处理,保持or表达式,因为内联表(LogicalValues)会被进一步'优化为'带有groupby的sql.



生成的sql遇上异常

```
SELECT list is not in GROUP BY clause and contains nonaggregated column 'xxx' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by
```



mysql设置,即不带only_full_group_by属性

```sql
SET GLOBAL sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

 SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
```









## 内置函数列表

原则上mycat不应该对函数运算,想要更多函数支持请提issue

##### 数学函数

https://github.com/MyCATApache/Mycat2/blob/08045e4fda1eb135d2e6a7029ef4bcc5b739563b/mycat2/src/test/java/io/mycat/sql/MathChecker.java

##### 日期函数

https://github.com/MyCATApache/Mycat2/blob/08045e4fda1eb135d2e6a7029ef4bcc5b739563b/mycat2/src/test/java/io/mycat/sql/DateChecker.java

##### 字符函数

https://github.com/MyCATApache/Mycat2/blob/70311cbed295f0a5f1a805c298993f88a6331765/mycat2/src/test/java/io/mycat/sql/CharChecker.java







## 读写分离配置

自动路由

https://github.com/MyCATApache/Mycat2/blob/master/example/src/test/resources/io/mycat/example/readWriteSeparation/mycat.yml



## 分片配置

https://github.com/MyCATApache/Mycat2/blob/master/example/src/test/resources/io/mycat/example/sharding/mycat.yml



## 高级内容



##### 多配置文件

-DMYCAT_HOME=mycat2\src\main\resources 指向的是配置文件夹

mycat.yml是主配置文件

而该文件夹下其他以yml为结尾的文件,mycat也会尝试加载并合拼到主配置对象里

合拼的单元是

```
metadata:
  schemas:[]
```



```
datasource:
  datasources:[]
```



```
interceptors:
  []
```



```
cluster: 
  clusters: []
```



能看到它们的配置都是以列表方式配置的,如果这些单元配置在副配置文件里,也会被合拼到主配置文件



### 拦截器与命令

#### SQL匹配

##### 匹配模式

以SQL词法token为分词单元,前缀匹配唯一项为基础,Mysql关键字为分隔符,设计的匹配器,设计上不支持多语句

##### 目的

在非支持所有情况的情况下(需要支持复杂情况请定制代码)

1. 为了简化一部分SQL改写需求,减少mycat解析命令的混乱情况

2. 迅速把不同的SQL交给不同的处理器执行

3. 自动处理use 语句和sql中的不带库的表信息的匹配

4. 显式的配置,明确哪些sql是怎样被mycat处理

   

拦截器处理流程

接收sql

->匹配器分析获得可能可以处理的命令及其配置并依次匹配执行(io.mycat.commands.MycatCommand)

->执行每个MycatCommand前执行命令中配置的io.mycat.Hint,其作用是提取信息保存到上下文中(Map)

->检查上下文中是否有缓存配置,如果缓存中有数据则返回缓存数据

->如果当前是explain语句,则执行MycatCommand的explain函数,否则执行run函数



io.mycat.Hint 

```java
public interface Hint {
    String getName();
    void accept(String buffer, Map<String, Object> t);
}
```



io.mycat.commands.MycatCommand

```java
public interface MycatCommand {

    boolean run(MycatRequest request, MycatDataContext context, Response response);

    boolean explain(MycatRequest request, MycatDataContext context, Response response);

    String getName();
}
```



Hint与MycatCommand都在Plug配置里加载



```yaml
plug:
  command:
    commands: 
     - {clazz: xxx , name: xxx}
  hint:
    hints: 
     - {clazz: xxx, name: xxx ,args:''}
  loadBalance:
    defaultLoadBalance: balanceRandom
    .....
```



##### 处理器基本形式

该配置同时也是默认处理器的配置

```yml
{command: 命令名 , tags: {参数名: 值,...}} #参数,键值对
//扩展
{command: 命令名 , tags: {参数名: 值,...},explain: 生成模板 , cache: 缓存配置 }
```



带sql匹配模式的配置

```yaml
{name: sql匹配器名字,
sql: 匹配模式 ,
command: 命令名, 
tags: {参数名: 值,...},
explain: 生成模板 , 
cache: 缓存配置
}
```



##### SQL匹配配置模板



拦截器配置模板

```yaml
interceptors: 
  [{拦截器},{拦截器}]
```



```yml
interceptors:
  [{user:{username: 'root' ,password: '123456' , ip: '.'},
    defaultHanlder: {command: mycatdb},
    sqls: [] , 
    sqlsGroup: [], #使用yaml语法引用一组匹配域
    transactionType: proxy  #xa,proxy 该用户连接时候使用的事务模式,
    matcherClazz: #匹配器的类名字
   }]
```



###### 默认Hanlder

当上述两种匹配器无法匹配的时候,走该分支

###### 匹配域

sqls与sqlsGroup实际上是同一个配置

sqls: `List<TextItemConfig>`

sqlsGroup:`List<List<TextItemConfig>>`

sqlsGroup 的存在是为了简化无表SQL的配置,这些SQL一般是客户端发出的事务控制语句等,繁琐,一般用户无需理会,所以可以利用yaml的锚标记把别处的配置引用至此

配置加载器会把sqlsGroup append到sqls



sql样例

```yaml
#lib start
sqlGroups:
  jdbcAdapter:
    sqls: &jdbcAdapter [
    {name: explain,sql: 'select 1' ,command: 'mycatdb' }, #explain一定要写库名
```

可以看出&jdbcAdapter对应上述*jdbcAdapter,不清楚的同学请看yaml的语法



###### lib域

```yaml
#lib start

#lib end
```

mycat的yaml配置加载器会在转换配置之前把这部分复制到文本头部,便于使用锚语法,原则上该域不应该有实际的语法单元,应该全是被用于锚的内容



###### transactionType

 txa,proxy 该用户连接时候使用的事务模式 

默认应该为proxy,此为mycat2.0最高性能的模式,但有一定使用约束



```yaml
interceptor: #拦截器,试用explain可以看到执行计划,查看路由
  defaultHanlder: {command: execute , tags: {targets: defaultDs,forceProxy: true}}
  sqls: [
  {name: useStatement; ,sql: 'use {schema};',command: useStatement}
  ]
  transactionType: xa #xa.proxy
```





#### 缓存

```yaml
  {sql: 'select 1  from db1.travelrecord',command: distributedQuery, cache: 'initialDelay = 1s,refreshInterval = 15s'}
```

initialDelay:mycat启动后多久开始预读

refreshInterval:刷新时间

单位:d,h,m,s

缓存的sql不能是带有通配符的

仅支持distributedQuery,executePlan

一个用户的配置对应一个拦截器,一个缓存对象管理器,一个拦截器对应多个sql匹配器,此缓存对象管理该用户配置下的sql缓存.也就是说即使是同一个sql,他们在不同的用户下,他的缓存配置是可以不同的.



## 自定义分片算法(单值)

单值分片算法抽象类

io.mycat.router.CustomRuleFunction

单值分片算法

io.mycat.router.SingleValueRuleFunction



例子

```
public class PartitionByLong extends SingleValueRuleFunction {

  private PartitionUtil partitionUtil;
  @Override
  public String name() {
    return "PartitionByLong";
  }

  @Override
  public void init(ShardingTableHandler table, Map<String, String> properties, Map<String, String> ranges) {
    int[] count = (toIntArray(properties.get("partitionCount")));
    int[] length = toIntArray(properties.get("partitionLength"));
    partitionUtil = new PartitionUtil(count, length);
  }

  @Override
  public int calculateIndex(String columnValue) {
    try {
      long key = Long.parseLong(columnValue);
      key = (key >>> 32) ^ key;
      return partitionUtil.partition(key);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "columnValue:" + columnValue + " Please eliminate any quote and non number within it.",
          e);
    }
  }

  @Override
  public int[] calculateIndexRange(String beginValue, String endValue) {
    ......
  }

  @Override
  public int getPartitionNum() {
    return partitionUtil.getPartitionNum();
  }
}

```



## 命令

**命令名大小写敏感**



##### mycatdb

该命令是根据分片配置自动处理sql



##### distributedQuery

使用metaData配置的信息处理查询语句

该命令的目标是自动分布式查询

支持for update语句

next_value_for('全局序列号名字')函数查询全局序列号

```yaml
{sql: 'select {any}',command: distributedQuery }
```



##### distributedInsert

使用metaData配置的信息处理insert语句

当建表语句的字段有自增信息,同时配置有全局序列号,将自动生成自增id

```
{sql: 'insert {any}',command: distributedInsert}
```

等价于

```yaml
{sql: 'insert {any}',command: execute, tags: {executeType: INSERT,getMetaData: true,needTransaction: true }},
```



##### distributedUpdate

使用metaData配置的信息处理update语句,注意的是,不能修改分片表的分片值

```yaml
{sql: 'update {any}',command: distributedUpdate}
```

等价于

```yaml
{sql: 'update {any}',command: execute,tags: {executeType: UPDATE,getMetaData: true ,needTransaction: true }},
```



##### distributedDelete

使用metaData配置的信息处理update语句,注意的是,不能修改分片表的分片值

```
{sql: 'delete {any}',command: distributedDelete}
```

等价于

```yaml
{sql: 'delete {any}',command: execute,tags: {executeType: UPDATE,getMetaData: true,needTransaction: true  }}
```



##### explain

查看sql执行计划

```yaml
{name: explain,sql: 'EXPLAIN {statement}' ,command: explainSQL}
```



##### boostMycatdb

```yaml
{sql: 'SELECT COUNT(1) FROM db1.travelrecord',command: boostMycatdb ,tags:{ boosters: defaultDs2 }}
```

在无事务且开启自动提交的情况下指定的sql发送到后端目标

**该命令不会改变sql**



参数

```
tags:{ boosters: defaultDs2 }
```

如果不配置参数

则使用拦截器上的配置boosters

也支持以列表方式配置

```
tags:{ boosters: [defaultDs2] }
```



##### executePlan

执行hbt

```yaml
name: hbt,sql: 'execute plan {hbt}' , explain: '{hbt}' ,command: executePlan
```



##### explainHbt

解释hbt

```yaml
{name: explainHbt,sql: 'EXPLAIN plan {hbt}' , explain: '{hbt}' ,command: explainPlan}
```



##### commit

```yaml
{name: commit,sql: 'commit',command: commit},{name: commit;,sql: 'commit;',command: commit},
```



##### begin

```yaml
{name: begin; ,sql: 'begin',command: begin},{name: begin ,sql: 'begin;',command: begin},
```



##### rollback

```yaml
{name: rollback ,sql: 'rollback',command: rollback},{name: rollback;,sql: 'rollback;',command: rollback},
```



##### 切换数据库

```yaml
{name: useStatement ,sql: 'use {schema}',command: useStatement},
```



##### 开启XA事务

```yaml
{name: setXA ,sql: 'setVariable xa = on',command: onXA},
```



##### 关闭XA事务

```yaml
{name: setProxy ,sql: 'setVariable xa = off',command: offXA},
```



##### 关闭自动提交

关闭自动提交后,在下一次sql将会自动开启事务,并不会释放后端连接

```yaml
{name: setAutoCommitOff ,sql: 'setVariable autocommit=off',command: setAutoCommitOff},
```



##### 开启自动提交

```yaml
{name: setAutoCommitOn ,sql: 'setVariable autocommit=on',command: setAutoCommitOn},
```



##### 设置事务隔离级别

```
READ UNCOMMITTED,READ COMMITTED,REPEATABLE READ,SERIALIZABLE
```

```yaml
{name: setTransactionIsolation ,sql: 'SET SESSION TRANSACTION ISOLATION LEVEL {transactionIsolation}',command: setTransactionIsolation},
```



##### 执行SQL

##### execute

##### 

###### forceProxy

true|false

默认值:false

强制SQL以proxy上运行,忽略当前事务



背景:

mycat有两种执行sql的形式,jdbc与native实现与mysql通讯



使用场景:

当处于使用jdbc事务的情况下,使用该属性可以忽略jdbc事务,使用native方式与mysql通讯.



###### needTransaction

true|false

默认值:true

根据上下文(关闭自动提交)自动开启事务



背景:

```
set autocommit = 0; 
此处select/delete/insert/update开启自动事务,这里称为首次操作语句
```

在分库分表结合透传的情况下,有技术限制

透传要求一个前端一个时刻对应一个后端,不论有没有通讯,此处限制绑定一个后端是为了简化后端状态管理.

如果用户或者客户端在首次操作语句的位置发了无法确定的数据源目标的sql,比如无表的sql,将会导致该随机获得的后端会话与前端的会话绑定直到事务消失。

而该needTransaction为false的时候，遇上autocommit = 0也不会自动开启事务，而needTransaction为true的时候，则自动开启事务



###### metaData

true|false

默认：false

true的时候不需要配置targets,自动根据分库分表配置路由sql

false的时候要配置targets，



###### targets

根据其他属性选择配置

sql发送的目标:集群或者数据源的名字

暂时targets只能配置一个值



###### balance

可空

当targets是集群名字的时候生效,使用该负载均衡策略



###### executeType

默认值:QUERY_MASTER



QUERY执行查询语句,在proxy透传下支持多语句,否则不支持

QUERY_MASTER执行查询语句,当目标是集群的时候路由到主节点

INSERT执行插入语句

UPDATE执行其他的更新语句,例如delete,update,setVariable



##### 返回ok packet

该命令主要是忽略该sql的处理,如果是查询语句,返回ok packet,客户端就会收到空的结果集,如果是非查询语句,则会收到正常的处理结果.

无参数

```yaml
{command: ok }
```



##### 返回error信息

```yaml
{command: error , tags: {errorMessage: "错误!",errorCode: -1}}
```



##### selectTransactionReadOnly

```yaml
{name: 'mysql SELECT @@session.transaction_read_only',sql: 'SELECT @@session.transaction_read_only',command: selectTransactionReadOnly , tags: {columnName: '@@session.transaction_read_only'}},
```



##### selectAutocommit

```yaml
{name: 'mysql SELECT @@session.autocommit', sql: 'SELECT @@session.autocommit',command: selectAutocommit},
```



##### selectLastInsertId

用于返回自增主键(全局表,全局序列号)

```yaml
{name: 'mysql SELECT  LAST_INSERT_ID()', sql: 'SELECT  LAST_INSERT_ID()',command: selectLastInsertId },
```



#### 匹配器实现

###### io.mycat.matcher.StringEqualsFactory

该实现使用字符串比较,没有文本提取功能



###### io.mycat.matcher.RobPikeMatcherFactory

RobPike的正则表达式实现,具体查看资料,没有文本提取功能



###### io.mycat.matcher.GPatternFactory

可以完成匹配提取参数功能,但是使用有较大限制,具体看资料



###### io.mycat.matcher.PatternFactory

java.util.regex.Pattern的匹配器实现



模式语法参考

https://github.com/MyCATApache/Mycat2/blob/master/doc/29-mycat-gpattern.md







##### 匹配器提取的文本的使用

提取是匹配器实现的,引用是mycat内置实现的,与匹配器没有关系



以io.mycat.matcher.GPatternFactory为例

###### 模板参数提取

sql中{name}是通配符,基于mysql的词法单元上匹配

同时把name保存在上下文中,作为命令的参数,所以命令的参数是可以从SQL中获得

```yaml
tags: {targets: defaultDs,forceProxy: true}
```

tags是配置文件中定下的命令参数

sql中的参数的优先级比tags高



###### SQL再生成

```yaml
  {name: 'mysql setVariable names utf8', sql: 'SET NAMES {utf8}',explian: 'SET NAMES utf8mb4'  command: execute , tags: {targets: defaultDs,forceProxy: true}}
```

SQL被'SET NAMES utf8mb4'替换



```yaml
  {name: 'select n', sql: 'select {n}',explain: 'select {n}+1' command: execute , tags: {targets: defaultDs,forceProxy: true}},
```

{}的模板是使用MessageFormat实现的



## 实验性sql



#### 客户端相关

##### ANALYZE TABLE

``` sql
ANALYZE TABLE schemaName.tableName;
ANALYZE TABLE db1.travelrecord;

//res
Table				Op			Msg_type	Msg_Text
db1.travelrecord	analyze		status		OK
```

当表名为mycat中配置的表名时候,mycat会对存储节点发送查询行数量的语句,统计该逻辑表中所有表并记录,行数可以帮助优化器进行算子优化



##### SHOW CREATE TABLE 

```sql
SHOW CREATE TABLE db1.travelrecord;

//res
Table	Create Table
travelrecord	CREATE TABLE `travelrecord` ( `id` bigint(20) NOT NULL AUTO_INCREMENT,`user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,`traveldate` date DEFAULT NULL,`fee` decimal(10,0) DEFAULT NULL,`days` int(11) DEFAULT NULL,`blob` longblob DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

当表名为mycat中配置的表名时候,mycat会把配置中的建表sql返回

当表名不存在时候,行为还没有定义(路由到第一个数据源)



##### SHOW DATABASES

```sql
SHOW DATABASES;

//res
Database
db1
```

mycat返回逻辑库信息



##### SHOW ENGINES

```
SHOW ENGINES

//res
Engine	Support	Comment
InnoDB	DRFAULT ...
```

mycat返回固定的引擎信息



##### SHOW TABLES

##### SHOW TABLE STATUS

```sql
SHOW TABLES;
SHOW TABLES FROM db1;

//res
Tables_in_db1
address1

SHOW TABLE STATUS;
SHOW TABLE STATUS  FROM db1;
```

sql中不带库名,如果mycat会话中设置了默认schema,则会对sql使用此schema作为库名补充

然后根据库名获取逻辑库上配置的targetName路由,如果没有配置则路由到一个节点(此行为可能会改变)

```sql
use db1;
SHOW TABLES;
//实际mycat向后端发送的是SHOW TABLES FROM db1;
```



#### Mycat2管理与监控相关

配置9066管理端

管理端只有manager的用户才可以使用

manager有独立的执行线程,一般不受8066的请求影响

**命令语法注意空格和分号**



```yml
manager:
  ip: 127.0.0.1
  port: 9066
  users: [{ip: '.', password: '123456', username: root}]

properties:
  key: value
```

mycat中创建的连接一般有两大类,前端连接,后端连接,后端连接也区分native连接与jdbc连接



##### 命令监控管理

###### 关闭连接

```sql
kill @@connection id1,id2...
```

id是mycat前端连接或者后端native连接的id(它们公用一个id生成器)

不能关闭jdbc连接,当关闭mycat前端连接的时候会自动关闭连接占用的jdbc连接



###### 显示Mycat前端连接

```sql
show @@connection
```



ID 连接的标识符

USER_NAME 登录的用户名

HOST 客户端连接地址



###### 显示native连接

```sql
show @@backend.native
```

显示mycat proxy native 连接的信息

SESSION_ID 连接ID,可被kill命令杀死

THREAD_NAME 所在线程名

DS_NAME数据源名字

LAST_MESSAGE 接收到的报文中的信息(错误信息)

MYCAT_SESSION_ID 如果有绑定前端连接,则显示它的ID

IS_IDLE 是否在连接池,即是否闲置

SELECT_LIMIT限制返回行数

IS_RESPONSE_FINISHED响应是否结束

RESPONSE_TYPE响应类型

IS_IN_TRANSACTION是否处于事务状态

IS_REQUEST_SUCCESS是否向后端数据库发起请求成功

IS_READ_ONLY是否处于readonly状态



###### 显示数据源状态

```sql
show @@backend.datasource
```

显示配置中的数据源信息



###### 显示心跳状态

```sql
show @@backend.heartbeat
```

显示配置中的心跳信息



###### 显示可以使用的管理命令

```sql
show @@help
```



###### 显示心跳中数据源实例中的状态

navite连接与jdbc连接使用相同的数据源配置,指向相同的服务器,那么它们的数据源实例只有一个

```sql
show @@backend.instance
```

NAME  数据源名字

ALIVE 是否存活

READABLE 是否可以选择为读节点

TYPE 数据源类型

SESSION_COUNT 当前连接数量

WEIGHT 负载均衡权重

MASTER是否主节点

HOST连接信息

PORT连接端口

LIMIT_SESSION_COUNT连接限制数量

REPLICA所在集群名字



###### 显示逻辑库配置

```sql
show @@metadata.schema
```

显示配置中的逻辑库信息



###### 显示逻辑表配置

```sql
show @@metadata.schema.table
```

显示配置中的逻辑表信息



###### 显示reactor线程状态

reactor是mycat2的io线程,主要处理透传响应与接收报文,解析sql等任务

```sql
show @@reactor
```

THREAD_NAME线程名字

THREAD_ID 线程ID

CUR_SESSION_ID当前正在处理的前端,后端会话ID

BUFFER_POOL_SNAPSHOT 网络缓冲区池快照

LAST_ACTIVE_TIME 最近活跃时间



###### 显示集群状态

```sql
show @@backend.replica
```



NAME 集群名字

SWITCH_TYPE 切换类型

MAX_REQUEST_COUNT 获取连接的时候尝试请求的次数

TYPE 集群类型

WRITE_DS 写节点列表

READ_DS 读节点列表

WRITE_L写节点负载均衡算法

READ_L读节点负载均衡算法





###### 显示定时器状态

```sql
show @@schedule
```



###### 显示sql统计信息

```sql
show @@stat
```



COMPILE_TIME 编译SQL的耗时

RBO_TIME 规则优化耗时

CBO_TIME 代价优化与生成执行器耗时

CONNECTION_POOL_TIME 连接池获取连接耗时

CONNECTION_QUERY_TIME 发起查询到获得响应耗时

EXECUTION_TIME 执行引擎耗时

TOTAL_TIME 查询总耗时



###### 重置sql统计信息

```sql
reset @@stat
```



###### 显示线程池状态

```sql
show @@threadPool
```



NAME 线程池名字

POOL_SIZE 线程最大数量

ACTIVE_COUNT 活跃线程数

TASK_QUEUE_SIZE 等待队列大小

COMPLETED_TASK 完成的任务数量

TOTAL_TASK 总任务数量



###### 设置数据源实例状态

```sql
switch @@backend.instance = {name:'xxx' ,alive:'true' ,readable:'true'} 
```

name是数据源名字

alive是数据源可用状态,值 true|false

readable是数据源可读状态,值 true|false

此命令供外部服务修改mycat里的数据源实例状态,可以以此支持多种集群服务



###### 集群切换

```
switch @@backend.replica = {name:'xxx'} 
```

name是数据源名字

手动触发集群切换

此命令供外部服务修改mycat里的数据源实例状态,可以以此支持多种集群服务



###### 心跳开关

```sql
switch @@backend.heartbeat = {true|false}
```

当有心跳配置的时候,可以进心跳进行开启关闭

心跳会自动修改数据源实例的状态,关闭心跳可以自行通过上面的命令修改状态

此命令供外部服务修改mycat里的数据源实例状态,可以以此支持多种集群服务



###### 显示心跳定时器是否正在运行

```sql
show @@backend.heartbeat.running
```



###### 配置更新

```sql
reload @@config by file
```

修改本地的mycat.yml就可更新,支持更新metadata与jdbc数据源.请在低峰时段执行,配置更新停止IO请求,尽量选择没有事务的一刻进行更新,不保证配置前后一致性等问题



###### 显示服务器信息

```
show @@server
```



##### Mycat2可视化监控

Mycat2可视化监控,使用Grafana和prometheus实现:

https://github.com/MyCATApache/Mycat2/blob/master/Mycat2-monitor.json

可配合模板JVM dashboard



###### 参考配置

https://github.com/MyCATApache/Mycat2/blob/master/example/src/test/resources/io/mycat/example/manager/mycat.yml



```yaml
plug:
  extra: [
           "io.mycat.exporter.PrometheusExporter"
     ]
```

此配置默认开启7066端口.并提供以下url供查询监控信息

http://127.0.0.1:7066/metrics

供Prometheus查询



```yaml
properties:
  prometheusPort: 7066
```

此配置可以更改io.mycat.exporter.PrometheusExporter开启的端口



###### 监控信息

Gauge类型

buffer_pool_counter:内存块计数

client_connection:客户端计数

native_mysql_connection:native连接计数

instance_connection:物理实例计数

jdbc_connection:jdbc连接计数

mycat_cpu_utility:cpu利用率

heartbeat_stat:心跳请求至响应时间

instance_acitve:物理实例存活是否存活

replica_available_value:集群是否可用

sql_stat:sql各阶段时间统计

thread_pool_active:连接池活跃线程统计



如果有什么建议可以提交issue或者与作者沟通



## 更新日志

具体看git记录

2020-5-5拦截器,元数据配置发生变更

2020-6-19后,mycat.yml中的server设置发生变化

2020-6-23后添加hint,MycatCommand配置

2020-6-30后添加extra配置