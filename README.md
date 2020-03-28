

# mycat 2.0-readme

author:junwen  2020-3-28

作者qq: 294712221

qq群:332702697

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

开发日志: <https://github.com/MyCATApache/Mycat2/blob/master/doc/101-todo-history-list.md>

项目地址:<https://github.com/MyCATApache/Mycat2>

HBTlang文档: <https://github.com/MyCATApache/Mycat2/blob/master/doc/103-HBTlang.md>

执行hbt的两组命令是

```sql
EXPLAIN SELECT id FROM db1.travelrecord WHERE id = 1;

EXECUTE plan fromSql(repli,'SELECT `id`  FROM `db1`.`travelrecord`  WHERE `id` = 1')
```




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



## 开发环境
参考src\main\resources\sql中的sql和src\main\resources\mycat.yml建立数据库环境
ide安装lombok插件
启动 io.mycat.MycatCore类



## 打包

```shell
mvn package
```

## 执行

只要使用2.0,都需要备份程序对应的源码,便于查找问题

```shell
.db1.sql,db2.sql,db3.sql是测试用的表
java -Dfile.encoding=UTF-8 -DMYCAT_HOME=mycat2\src\main\resources  -jar mycat2-0.5-SNAPSHOP.jar

Mycat2\mycat2\src\main\resources 是mycat.yml所在的文件夹
配置加载可以通过替换
io.mycat.ConfigProvider实现不同的配置加载方式
```



## Mycat2流程

客户端发送SQL到mycat2,mycat2拦截对应的SQL执行不同的命令,对于不需要拦截处理的SQL,透传到有逻辑表的mysql,这样,mycat2对外就伪装成mysql数据库



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

### 

####   Mycat2的分片分库分表运算

​		在分片分库分表中运算分为两个部分,一部分是后端每个数据库的运算,这部分运算以SQL作为中间语言发送到后端服务器,一部分以HBT形式在mycat里执行,占用内存主要是驻留的结果集的总大小.如果结果集合拼的结果行是固定行,固定列,结果集每个值长度也是固定的,那意味着运算都是reduce的,可以边运算边丢弃已处理的值,无需保存完整的后端处理结果.

​		当mycat2无法下推大部分运算的时候(主要是join,后面会继续优化),则可能拉取大结果集,处理还是很耗时的.所以尽量使用分片谓词靠近数据源风格编写SQL,便于mycat2识别可下推的谓词.



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
metadata: #元数据 升级计划:通过创建表的sql语句提供该信息免去繁琐配置,
  prototype: {targetName: defaultDs } #从该数据源名字获取配置,该功能未开放
  schemas:
    db1: #逻辑库名
      shadingTables: #分片表信息
        travelrecord: #此处写配置的表信息
        address: #此处写配置的表信息
      globalTables: #全局表信息
        company: #此处写配置的表信息
```



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

2.当建表sql中的字段信息带有AUTO_INCREMENT同时配置中有配置全局序列号,则该sql在插入数据的时候,自动改写sql补上自增值

3.要么是自然分片要么是动态分片



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

密码:区分大小写

ip:用户连接的远程ip接收的格式是

```
127.0.0.1:8888
```

支持正则表达式匹配



#### SQL匹配

##### 匹配模式

以SQL词法token为分词单元,前缀匹配唯一项为基础,Mysql关键字为分隔符,设计的匹配器,设计上不支持多语句

##### 目的

在非支持所有情况的情况下(需要支持复杂情况请定制代码)

1. 为了简化一部分SQL改写需求,减少mycat解析命令的混乱情况

2. 迅速把不同的SQL交给不同的处理器执行

3. 自动处理use 语句和sql中的不带库的表信息的匹配

4. 显式的配置,明确哪些sql是怎样被mycat处理

   

##### 处理器基本形式

该配置同时也是默认处理器的配置

```yml
{command: 命令名 , tags: {参数名: 值,...}} #参数,键值对
//扩展
{command: 命令名 , tags: {参数名: 值,...},explain: 生成模板 , cache: 缓存配置 }
```

###### 

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
    defaultHanlder: {command: execute , tags: {targets: defaultDs,forceProxy: true}},
    schemas: [{
                tables:[ 'db1.travelrecord','db1.address','db1.company'],
                sqls: [
                {sql: 'select {any}',command: distributedQuery }, #带表sql匹配Hanlder
                {sql: 'insert {any}',command: distributedInsert},
                {sql: 'update {any}',command: distributedUpdate},
                {sql: 'delete {any}',command: distributedDelete},
                ],
              },
    ],
    sqls: [] , #不带表名匹配域
    sqlsGroup: [*jdbcAdapter],
    transactionType: proxy  #xa,proxy 该用户连接时候使用的事务模式 
   }]
```

###### 表名匹配

```
tables:[ '库名.表名',...]
```

中的表名只要在sql中出现,就会进入对应sqls的匹配流程

###### 默认Hanlder

当上述两种匹配器无法匹配的时候,走该分支

###### 不带表名匹配域

sqls与sqlsGroup实际上是同一个配置

sqls: `List<TextItemConfig>`

sqlsGroup:`List<List<TextItemConfig>>`

sqlsGroup 的存在是为了简化无表SQL的配置,这些SQL一般是客户端发出的事务控制语句等,繁琐,一般用户无需理会,所以可以利用yaml的锚标记把别处的配置引用至此

配置加载器会把sqlsGroup append到sqls



无表sql样例

```yaml
#lib start
sqlGroups:
  jdbcAdapter:
    sqls: &jdbcAdapter [
    {name: explain,sql: 'EXPLAIN {statement}' ,command: explainSQL}, #explain一定要写库名
    {name: hbt,sql: 'execute plan {hbt}' , explain: '{hbt}' ,command: executePlan},#执行hbt
    {name: commit,sql: 'commit',command: commit},{name: commit;,sql: 'commit;',command: commit},
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
interceptor: #拦截器,如果拦截不了,尝试use schema,试用explain可以看到执行计划,查看路由
  defaultHanlder: {command: execute , tags: {targets: defaultDs,forceProxy: true}}
  schemas: [{
              tables:[ 'db1.travelrecord','db1.address1'],#sql中包含一个表名,就进入该匹配流程
              sqls: [
              {sql: 'select {selectItems} from {any}',command: distributedQuery },
              {sql: 'delete {any}',command: execute,....}
              ],
            },
            {
              tables:[ 'db1.company'],
              sqls: [
              {sql: 'select {selectItems} from {any}',command: execute ,....}
              ],
            },
  ]
  sqls: [
  {name: useStatement; ,sql: 'use {schema};',command: useStatement}
  ]
  transactionType: xa #xa.proxy
```



#### 模式语法参考

https://github.com/MyCATApache/Mycat2/blob/master/doc/29-mycat-gpattern.md



#### 模板参数提取

sql中{name}是通配符,基于mysql的词法单元上匹配

同时把name保存在上下文中,作为命令的参数,所以命令的参数是可以从SQL中获得

```yaml
tags: {targets: defaultDs,forceProxy: true}
```

tags是配置文件中定下的命令参数

sql中的参数的优先级比tags高



#### SQL再生成

```yaml
  {name: 'mysql set names utf8', sql: 'SET NAMES {utf8}',explian: 'SET NAMES utf8mb4'  command: execute , tags: {targets: defaultDs,forceProxy: true}}
```

SQL被'SET NAMES utf8mb4'替换



```yaml
  {name: 'select n', sql: 'select {n}',explain: 'select {n}+1' command: execute , tags: {targets: defaultDs,forceProxy: true}},
```



拦截器会对use {schema}语句处理,得出不带schema的sql的table是属于哪一个schema.当mycat2发生错误的时候,会关闭连接,此时保存的schema失效,重新连接的时候请重新执行use schema语句



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



## 命令

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
{name: setXA ,sql: 'set xa = on',command: onXA},
```



##### 关闭XA事务

```yaml
{name: setProxy ,sql: 'set xa = off',command: offXA},
```



##### 关闭自动提交

关闭自动提交后,在下一次sql将会自动开启事务,并不会释放后端连接

```yaml
{name: setAutoCommitOff ,sql: 'set autocommit=off',command: setAutoCommitOff},
```



##### 开启自动提交

```yaml
{name: setAutoCommitOn ,sql: 'set autocommit=on',command: setAutoCommitOn},
```



##### 设置事务隔离级别

```
READ UNCOMMITTED,READ COMMITTED,REPEATABLE READ,SERIALIZABLE
```

```yaml
{name: setTransactionIsolation ,sql: 'SET SESSION TRANSACTION ISOLATION LEVEL {transactionIsolation}',command: setTransactionIsolation},
```



##### execute执行SQL

forceProxy:true|false

强制SQL以proxy上运行,忽略当前事务



needTransaction:true|false

根据上下文(关闭自动提交)自动开启事务



metaData:true|false

true的时候不需要配置targets,自动根据sql路由

false的时候要配置targets



targets

sql发送的目标:集群或者数据源的名字



balance

当targets是集群名字的时候生效,使用该负载均衡策略



executeType

QUERY执行查询语句,在proxy透传下支持多语句,否则不支持

QUERY_MASTER执行查询语句,当目标是集群的时候路由到主节点

INSERT执行插入语句

UPDATE执行其他的更新语句,例如delete,update,set



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
   initSQL: 'use db1', #建立连接后执行的sql,在此可以写上use xxx初始化默认database,该配置可能无效
    jdbcDriverClass: , #jdbc驱动
   instanceType: ,#READ,WRITE,READ_WRITE ,集群信息中是主节点,则默认为读写,副本则为读,此属性可以强制指定可写
  }
  ]
  datasourceProviderClass: io.mycat.datasource.jdbc.datasourceProvider.AtomikosDatasourceProvider
  timer: {initialDelay: 1000, period: 5, timeUnit: SECONDS}
```



maxConnectTimeout:单位millis

配置中的定时器主要作用是定时检查闲置连接



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
              reuqestType: 'mysql' #进行心跳的方式,mysql或者jdbc两种
   }}
  ]
  timer: {initialDelay: 1000, period: 5, timeUnit: SECONDS} #心跳定时器
```

只有GARELA_CLUSTER能在masters属性配置多个数据源的名字

reuqestType是进行心跳的实现方式,使用mysql意味着使用proxy方式进行,能异步地进行心跳,而jdbc方式会占用线程池

当配置是主从的时候,发生主从切换,mycat会备份原来的配置(文件名带有版本号)然后使用更新配置



## 服务器配置

```yaml
server:
  ip: 0.0.0.0
  port: 8066
  reactorNumber: 1
  #用于多线程任务的线程池,
  worker: {close: false, #禁用多线程池,jdbc等功能将不能使用
           maxPengdingLimit: 65535, #每个线程处理任务队列的最大长度
           maxThread: 2,
           minThread: 2,
           timeUnit: SECONDS, #超时单位
           waitTaskTimeout: 5 #超时后将结束闲置的线程
  }
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



### io.mycat.plug.sequence.SequenceSnowflakeGenerator

workerId对应雪花算法的参数



## Mycat2.0分布式查询支持语法

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
      [ FOR SYSTEM_TIME AS OF expression ]
      [ matchRecognize ]
      [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'
  |   [ LATERAL ] '(' query ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

columnDecl:
      column type [ NOT NULL ]

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'
  |   GROUPING SETS '(' groupItem [, groupItem ]* ')'

```

## Mycat2.0分布式修改支持语法

暂时仅仅改写对应的表名和根据条件拆分sql,具体使用explain语句查看



## 常见优化

```sql
USE db1;

EXPLAIN SELECT id  FROM travelrecord WHERE id =1;

MycatTransientSQLTableScan(sql=[SELECT `id`  FROM `db1`.`travelrecord`  WHERE `id` = 1])


EXPLAIN SELECT COUNT(*)  FROM travelrecord WHERE id >=0;

LogicalAggregate(group=[{}], EXPR$0=[COUNT()])
  LogicalUnion(all=[true])
    MycatTransientSQLTableScan(sql=[SELECT COUNT(*)  FROM `db2`.`travelrecord`  WHERE `id` >= 0])
    MycatTransientSQLTableScan(sql=[SELECT COUNT(*)  FROM (SELECT *  FROM `db1`.`travelrecord`  WHERE `id` >= 0  UNION ALL  SELECT *  FROM `db1`.`travelrecord2`  WHERE `id` >= 0  UNION ALL  SELECT *  FROM `db1`.`travelrecord3`  WHERE `id` >= 0) AS `t2`])
    

EXPLAIN SELECT COUNT(*)  FROM travelrecord WHERE id >=0;

LogicalProject(sm=[$0], EXPR$1=[CASE(=($2, 0), null:BIGINT, $1)], EXPR$2=[/(CAST(CASE(=($2, 0), null:BIGINT, $1)):DOUBLE, $2)])
  LogicalAggregate(group=[{}], sm=[COUNT()], EXPR$1=[$SUM0($0)], agg#2=[COUNT($0)])
    LogicalUnion(all=[true])
      MycatTransientSQLTableScan(sql=[SELECT `id`  FROM `db2`.`travelrecord`  WHERE `id` >= 0])
      MycatTransientSQLTableScan(sql=[SELECT `id`  FROM `db1`.`travelrecord`  WHERE `id` >= 0  UNION ALL  SELECT `id`  FROM `db1`.`travelrecord2`  WHERE `id` >= 0  UNION ALL  SELECT `id`  FROM `db1`.`travelrecord3`  WHERE `id` >= 0])


```



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



## 常见备用配置模板

读写分离

```yaml
          {
             tables:[ 'db1.company'],
             sqls: [
             {sql: 'select {any}',command: execute ,tags: {targets: repli,executeType: QUERY ,needTransaction: true}},
             {sql: 'select {any} for update',command: execute ,tags: {executeType: QUERY_MASTER ,targets: repli,needTransaction: true}},
             {sql: 'insert {any}',command: execute, tags: {executeType: UPDATE ,targets: repli,needTransaction: true,}},
             {sql: 'delete {any}',command: execute, tags: {executeType: UPDATE ,targets: repli,needTransaction: true,}}
             ],
           },
```



## 已知限制

###### proxy事务模式

开启事务后的操作只能是同一个分片

事务里使用全局表会出现非同一分片的全局表无法回滚的现象

对于这种更新操作,,要求强一致性,可以开启xa



###### 分布式查询引擎

1. 结果集字段名不写别名的情况下,生成的列名是不确定的

2. sql不写order的情况下,结果集可能是未经排序的

3. 不建议写类似sql,sql中没有引用表的列名,这种sql在mycat里未正式支持(0.8版本后可以运行)

   `select 1 from db1.travelrecord where id = 1 limit 1`

4. sql一般带有分片条件,而且位于表名后的where,而且是简单的形式,复杂的条件和不写条件都会导致全表扫描

5. sql函数名不能出现Crudate的情况,否则无法识别

   



###### 更新日志

具体看git记录