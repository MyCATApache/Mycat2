#### 任务列表

1. 代码功能完成并有相应的修改说明,就可以记录到此文件,并留下指向github账户的链接和完成日期

2. 开发环境使用以JDK8为最低版本,但是仅使用JDK8 API开发

3. 测试环境

   服务器10.3.15-MariaDB对应MySQL5.7

   客户端:mysql-connector-java-8.0.11,mariadb-java-client 2.5.2



##### 进行中
HBT功能相关



##### 拦截器匹配需求收集

一种针对sql的匹配器,主要是mysql,但完备情况上不限sql方言

1. 支持sql词法,主要是mysql
2. 支持多语句,以;分隔的多个sql的字符串,匹配模式中的;原则上不影响多语句中每个sql的匹配,如果不支持这个特性,那意味着每个匹配模式都要写两个版本,一个带;一个不带
3. 一个匹配模式对应一个sql
4. 支持通配符 {xx}是字符串捕获,在匹配模式中select id from {xxxx} 可以对应select id from travelrecord 或者 select id from travelrecord where id = 1;此时xxx是travelrecord 或者travelrecord where id = 1
5. 支持?词法单元捕获,参考预处理的匹配模式,此单元原则上就是mysql词法单元的一个字面量,对应数值类型,字符等,而不是1+1(表达式)这类的非字面量
6. 支持匹配条件,比如 id = 1 or user = 1或者 id = 1 and user = 1可以提供一种模式语法提取逻辑运算符,例如 id = 1 #{condition } user = 1
7. 支持按表名分组匹配,提取表名
8. 支持按字段分组匹配
9. 高性能,sql的长度可能是很长的,对于一些常用sql可以瞬速预判,然后走特定的匹配场景
10. 输出结果应该是带有原sql的下标的,方便改写
11. 支持byte基本的解析,无需转成String再匹配,byte是按照utf8编码的
12. 支持中文
13. sql注释可以开启可以提取也是很重要
14. 检查到匹配模式有冲突在启动时候给与提示
15. 动态更新


上述功能不要求全部实现,尽可能实现即可



------

##### 备忘录



###### 多数据源查询语句特定方言(oracle)

HBT与普通sql执行生成目标数据源的sql不只是mysql方言,而是可以根据配置生成不同风格的sql



###### 结果集缓存动态调度

支持条件触发缓存结果集

条件1:低频率访问

条件2:响应时延大

执行特点:是否惰性触发




###### 预处理语句

潜在问题:

预处理使mycat代理架构变成带有状态的代理,暂时建议使用客户端的预处理方式(默认)

代码现状:

已经完成对sql分析生成预处理语句对象(结果集字段信息与?参数信息并与具体值结合执行),未与网络协议层整合



###### MySQL协议Ok packet 事务状态跟踪

1.记录Session change type日志

2.分析错误状态的session,并在连接池中去掉



###### loaddata大批量数据导入

1.前端协议支持loaddata

2.自动分析loaddata中的sql结合元数据自动完成数据导入



###### *information_schema* ()

1.模仿MySQL服务器把元数据表现成*information_schema* 对象(完成)

2.分布式查询引擎能对*information_schema*对象进行sql条件进行查询(已经完成)

3.DDL能对*information_schema*对象进行修改以及生成对多个数据源的更改sql(已经完成create table)

4.实现常用的show语句(实现一部分,但是某些客户端兼容性不好)



###### 数据迁移

1.根据分片算法不同的特性,计算迁移的最小数据任务

2.执行迁移





###### buffer pool

1.能检查buffer泄漏,并修复错误状态

2.能区分MappedByteBuffer与HeapByteBuffer,DirectByteBuffer





###### 配置中心

配置可视化



###### MySQL内置函数适配



###### 全局二级索引



###### 全局一致性检测



###### SQL执行信息统计

1.编译阶段,优化阶段,执行节点的统计已经完成

2.缺算子执行代价统计



###### 多租户分片专用路由命令



###### 分布式join



###### HBT提供完善的语法错误提示



###### 统计中心,为优化器选择物理执行器提供支持

已经完成行统计

缺:统计执行阶段的耗时,统计多次出现的拉取数据源的sql



2020.7.1-> 2020.7.5暂定开发计划

重新引入服务器预处理处理器



2020.6.29-> 2020.6.30开发计划

实现可视化监控端

实现mycat2整体的cpu,内存监控





##### 2020.6.19-> 2020.6.28日志

更改worker配置,重构线程池以便添加线程池监控(完成)

尝试实现自定义执行器接口(完成实验)

完成管理,监控命令(完成)

实现union all语法(完成)

配置动态更新,可以从配置中心拉取配置(完成,实验)

SQL改写注解(添加完成,添加文档描述)



##### 2020.6.15-> 2020.6.19日志

[配置无需配置建表sql](https://github.com/MyCATApache/Mycat2/commit/ec034439a43ec97da1e5e74668c4fad7d4f19225) 自动从dataNode中查询建表语句

[简化配置 ShardingType可以不写,默认自然分片](https://github.com/MyCATApache/Mycat2/commit/706262328682d490c850742f6f4c70d46c0e39ab)

[添加插入语句中出现未知字段的提示](https://github.com/MyCATApache/Mycat2/commit/d1bf9a01caa3440928f49427a48264a27802e9c0)

[修复caclite 内部类型为数字的日期类型 类型转换问题](https://github.com/MyCATApache/Mycat2/commit/c883f60366d69acc6048995b653d3b5c50e52728)

[通过判断是否存在初始化语句加速jdbc获取连接](https://github.com/MyCATApache/Mycat2/commit/b1438d34e698f8ab884675319a5f16a715da98c7)

[修复boolean类型转换](https://github.com/MyCATApache/Mycat2/commit/8d9bf7e9ed2abfbfd74bf917695bac350e3f53c7)

[show @@backend.replica](https://github.com/MyCATApache/Mycat2/commit/d7285cfd4eb24d8b9713a73578fa0e99f4b4c78f)

[show @@backend.datasource 可以显示连接使用数量](https://github.com/MyCATApache/Mycat2/commit/e3aa295288ed15a2b782941ba042a5cf4d3e43fa)

[实现show @](https://github.com/MyCATApache/Mycat2/commit/18f5bc104794a1b9ba1764125c156793c5099bfd)[@connection](https://github.com/connection)

[实现show @@backend.native](https://github.com/MyCATApache/Mycat2/commit/1f0467131d4b30fc548fc0c66811bc8414cbec99)



##### 2020.6.8-> 2020.6.14日志

修复读写分离在跨库情况下有异常

支持枚举类型(以字符串对待)

修复时间类型转换错误

修复sqllog下mycat自研的show column命令导致的崩溃

重构路由,可以支持分片算法返回dataNode

强制native(proxy)使用[NativePassword](https://github.com/MyCATApache/Mycat2/commit/a135181457554199aa072a8f8f097faa6cefa15e)插件,而不再自动可以切换为其他插件

使用druid作为数据源的时候可以使用本地事务

添加本地事务配置例子(druid)

修复使用jdbc查询返回[IllegalArgumentException();](https://github.com/MyCATApache/Mycat2/commit/9afe0dde62912da22839cf040bbc9e06690a5d32)

[忽略SET TRANSACTION READ WRITE;](https://github.com/MyCATApache/Mycat2/commit/1aa50549d1ce4f4467150065314739306cb7ea42)

[带有InformationSchema的表的sql发送到后端数据库](https://github.com/MyCATApache/Mycat2/commit/f3ee1fa64d27ae472dc095479a0f93cefd6c8b4b)

[修复xa测试主键冲突](https://github.com/MyCATApache/Mycat2/commit/6c5cc719ccc075a4828a208b560d52dc8735924b)

[暂时禁用show tables因为mysql8客户端不兼容此实现](https://github.com/MyCATApache/Mycat2/commit/6926e67a5b84946a83343f6be61c0ffd6acc23f1)

[禁用jdbc设置readOnly](https://github.com/MyCATApache/Mycat2/commit/6f9ce86a21c8d2221ff561522dd40a5b64cb2455)

[禁用UnionPullUpConstantsRule](https://github.com/MyCATApache/Mycat2/commit/00454390f52b34c6bb9535347fb6cc0d51d05374)





##### 2020.6.2-> 2020.6.7开发日志

迁移1.6路由(未完成)

limit/order下推

全局表与分片表精准下推



##### 2020.6.1-> 2020.6.2开发日志

完成booster功能



##### 2020.5.7-> 2020.5.30开发日志

完善自动路由

jdbc获取连接并行化

实现show databases

实现show tables

支持只使用JDBC作为后端连接

适配mysql workbench

修复jdbc并行后拉取多于两个数据源的连接的结果集是空的情况



##### 2020.4.26 -> 2020.5.6开发日志

以schema为根设计拦截器,默认不需要配置命令,实现自动化路由,但可以使用正则完全覆盖自动的路由的功能(未完成)



###### Set语句支持

1.使用set语句能对mycat中的MySQL动态变量进行修改,查询

2.但是仅对客户端相关的变量进行处理,不再把不能处理的set语句直接透传到MySQL后端服务器



###### 拦截器与命令重构



###### SQL匹配支持多语句(优先级很低,废弃)

多语句会加大SQL匹配和执行代码的复杂程度,因为Mycat2存在两种执行sql的方式,proxy和jdbc,用户要能完全保证只只用其中一个方式并满足约束.





###### 根据库名指定到分片

代码现状:需要把库中每个表都配置,并指定每个表sql的行为



##### 2020.4.19 -> 2020.4.26开发日志

修复1.03的一些bug,具体看1.04的更新日志



发现mycat2.0不能采用完全下推的方法,需要在mycat2使用并行拉取RDS数据,具体办法有两个方向

研究以最大并行为目标的优化方向的关系表达式上拉规则

迁移1.6的路由作为2.0的AutoHandler模块

用户可以选择HBT模块或者AutoHandler模块作为路由



以schema为根设计拦截器,默认不需要配置命令,实现自动化路由,但可以使用正则完全覆盖自动的路由的功能(未完成)



##### 2020.4.12 -> 2020.4.18开发日志

以schema为根设计拦截器,默认不需要配置命令,实现自动化路由,但可以使用正则完全覆盖自动的路由的功能(未完成)



##### 2020.4.6 -> 2020.4.11开发日志

修复若干bug

[修复客户端偶然不能登录](https://github.com/MyCATApache/Mycat2/commit/d75000e62ac05eab2f314ab17359a2e6fdcedd49)

[添加小型正则表达式实现](https://github.com/MyCATApache/Mycat2/commit/103a8dee2e8fa6f26f82d6fc86e8b6b08c51261d)

[提交读写分离测试](https://github.com/MyCATApache/Mycat2/commit/7ef024e067bc53bf7a096de3028897cb921ce019)

[修复单独使用SELECT next_value_for('db1_travelrecord')无法获得全局序列号](https://github.com/MyCATApache/Mycat2/commit/6f34d8eea1a77d372fb0ab2f864380f2fce8beee)

[修复读写分离,executeType query master路由到非主节点](https://github.com/MyCATApache/Mycat2/commit/292ed2fdfbfd75517e9f9781b421e97b85d72bb9)

##### 2020.4.1 -> 2020.4.5开发日志

1.完善mysql内部函数,主要是与分片算法相关的函数,便于生成分片值

2.hbt能对不写字段信息的sql,从数据源获取字段信息,提供语法错误提示

3.为常见的业务场景建立例子文件夹,便于学习

4.编写HBT新手教程(未完成)

5.修复hbt关联连接生成sql错误的问题,暂定关联连接无法下推



##### 2020.3.27 -> 2020.3.31开发日志

1.完善文档

2.测试,并发布一个release版本





##### 2020.3.23 -> 2020.3.30开发日志

使用元数据配置实现全局表,可配置选择proxy/xa形式写入多个目标

https://github.com/MyCATApache/Mycat2/commit/b222ab5eb69b5c229747ac08522c45254426016a

https://github.com/MyCATApache/Mycat2/commit/3836ad279081b1d0913599fad1b09d13147661fe

https://github.com/MyCATApache/Mycat2/commit/d64d155b886582aed8f3003478c1f8f154887522

使用元数据配置实现ER表(废弃)

分布式查询命令能生成for update语句

https://github.com/MyCATApache/Mycat2/commit/d141df1473f607983b90f16db7b55bd16447635a

使用元数据实现全局序列号

https://github.com/MyCATApache/Mycat2/commit/a463f53709d28fecd1f8018b462be59d2270ce5a

实现自增序列

https://github.com/MyCATApache/Mycat2/commit/0e73b4053dd24666b2e0b2f98de53ab2c017a27d

实现登录权限

https://github.com/MyCATApache/Mycat2/commit/0f02bb461c4353af8f041b93dc595134e38a7f75

[添加ok,distributedInsert,distributedUpdate命令](https://github.com/MyCATApache/Mycat2/commit/08ebdd28110515f45072525132373a1302b2bd7d)

简化配置

https://github.com/MyCATApache/Mycat2/commit/3c516f1ee2193e479197ca5a9ce437cf88d643df

升级calcite到1.22.0

https://github.com/MyCATApache/Mycat2/commit/9cd5e357617e0f0240c66da302b64ce8e9e069f4

修复不支持not表达式

https://github.com/MyCATApache/Mycat2/commit/977b52826bd42cbd20d18f391a69c2935e7de19f

添加部分sql表达式支持列表

https://github.com/MyCATApache/Mycat2/commit/106d61a0136376d216a2b808ff836c558ffe7963

https://github.com/MyCATApache/Mycat2/commit/08045e4fda1eb135d2e6a7029ef4bcc5b739563b

https://github.com/MyCATApache/Mycat2/commit/6f76f1a2c6550b21e6197b5ea6eb0ca714aa3900

修复生成ok包工具,autocommit设置成存在事务导致事务异常

https://github.com/MyCATApache/Mycat2/commit/a3039459e085112be116ffced34d108f7928286f

修复结果集的row阶段出现错误包后没有继续透传的错误

https://github.com/MyCATApache/Mycat2/commit/2023ccfcfea6734739777c4f050ac986e4560d21

##### 2020.3.16 -> 2020.3.23开发日志

1. HBT支持配置基于对象生成内部表,该功能用于辅助测试

   https://github.com/MyCATApache/Mycat2/blob/efe940c3dc9405e34a6a48da196d39440bdc52b5/hbt/src/main/java/io/mycat/upondb/MycatDBClientBasedConfig.java

2. 提交HBT基本测试例子

   https://github.com/MyCATApache/Mycat2/blob/43727229222deb735e6b43645e3c6c6e71f304b5/hbt/src/test/java/io/mycat/hbt/HBTBaseTest.java

3. 输出HBT文档

   https://github.com/MyCATApache/Mycat2/blob/2c88f6e7eee4f2fc7d7a7d2720f7d2eded932849/doc/103-HBTlang.md

4. 结果集缓存管理(包括启动时候执行定时任务,定期执行,结果集缓存任务,直接返还结果集)

   https://github.com/MyCATApache/Mycat2/blob/3ea3824c30c01bc4510d896b93c3536de24a85c3/mycat2/src/main/java/io/mycat/boost/BoostRuntime.java

5. SQL匹配器实验性支持select {any}与select 1模式混用

   https://github.com/MyCATApache/Mycat2/blob/e1b265d0a661e58d435a50eda8c9c60a44568226/pattern/src/test/java/io/mycat/pattern/GPatternRuleTest.java

6. 改善数据源选择的代码,为分布式查询中分片表与全局表命中同一个分片打基础,全局表未完成

   https://github.com/MyCATApache/Mycat2/blob/f9bf536e39ee765b6dfaf0f819c2a12ed225020a/replica/src/main/java/io/mycat/replica/DataSourceNearnessImpl.java

7. 主从切换index文件本地更新，下次启动的时候加载

   https://github.com/MyCATApache/Mycat2/commit/02c6701e3f4ff55bbfc06eda259fac55c138f240

   

   

##### 2020.3.9 -> 2020.3.15开发日志
https://github.com/MyCATApache/Mycat2/commit/86df5c18bb5bfeb2608c3f952175f6dcb93371dd

1. HBT能提供对实际执行计划输出功能
其中explain输出底层的迭代器的字段类型,用于调试时候能看到每个语法节点的类型

https://github.com/MyCATApache/Mycat2/commit/cc3ec77b1b075c4616bd096362370b8c3530fec8
https://github.com/MyCATApache/Mycat2/commit/7c0349a97a71dfcd12134713ec3ab9daf9ceefd3

2. 重构mycat 2.0 NIO框架以便支持3种线程事务模式(事务与线程绑定,非绑定,reactor),为支持更多事务框架做支持,两种写入模式(透传,自定义报文写入)

https://github.com/MyCATApache/Mycat2/commit/f3189bb071cb7514695b357b72c5e49e0d3b130f

3. 添加前端异常连接检测,IO超时检查(辅助测试,辅助释放资源)
https://github.com/MyCATApache/Mycat2/commit/78b6103b4497c0a1ea7f3922ebe1a7dab1fd80d8

4. @wangzihaogithub提交简单的数据源读取写入代码
https://github.com/MyCATApache/Mycat2/commit/51b98bf63428e0341827f017db045fb0afccc30d

待完善任务

1. 绑定事务的线程池优化,浪费一个监控线程维护等待的事务

2. 绑定事务线程与多线程报文写入方式优雅实现

3. 完善自定义报文缓存写入,已有原型,未整合

   




##### 2020年3月中旬前已完成

- [x] 重构HBT相关,重写HBT的文本解析器,使用多种工具集成HBT(junwen12221-2020-3-9)

- [x] 分布式查询能够下推SQL,三种常用下推优化,部分聚合下推,求最顶层下推运算节点,多次拉取数据的SQL的结果集变为一次(junwen12221-2020-2-15)

- [x] 设计新拦截器配置,公测(junwen12221-2020-1-15)



------



##### 2019年已完成

- [x] 静态注解实现([junwen12221](https://github.com/junwen12221)-2019-6-5,Deprecated)

- [x] 添加嵌入的mysql数据库用于测试(junwen12221-2019-6-14,Deprecated)

- [x] 负载均衡算法说明文档(zwy-2019-6-13)

- [x] 监控bufferpool(cjw-2019-6-15,Deprecated)

- [x] 监控sessionManager(cjw-2019-6-15)

- [x] NIO空轮训问题(cjw-2019-6-21)

- [x] 完善mycat对jdbc内部sql处理(cjw-2019-6-15)

- [x] 自定义mysql变量配置文件(show variables以及相应配置在mycat里生效,比如报文大小限制)(cjw-2019-6-15)

- [x] loaddata命令(试验) (cjw-2019-6-20)

- [x] 预处理命令命令(试验)(cjw-2019-6-20)

- [x] 游标(试验)(cjw-2019-6-21)

- [x] 数据源初始化SQL(cjw-2019-6-21)

- [x] 热重启接口(用于配置更新)(cjw-2019-6-23)

- [x] 命令处理接口可配置(cjw-2019-6-23)

- [x] 集群多主配置(cjw-2019-6-23)

- [x] loaddata路由不实现,路由节点取决于loaddata查询语句的路由结果(cjw-2019-6-24,Deprecated)

- [x] 预处理路由不实现,路由节点取决于预处理语句模板的路由结果(cjw-2019-6-24)

- [x] 添加Grid模块,准备用于二阶段开发计划(cjw-2019-6-28),移除RPC模块

- [x] 支持IO线程或者worker线程写入响应报文(cjw-2019-6-29)

- [x] 替换logger类为mycat专用logger,编写收集代码中的日志字符串初步的工具,移除logtip模块(cjw-2019-6-30)

- [x] 添加proxy 路由说明文档,jdbc内部sql说明文档,添加NIO JOB异常处理,路由重构(cjw-2019-7-3,Deprecated)

- [x] 根据报文标志判断只读事务(cjw-2019-7-4)

- [x] 在mysql proxy中实现mysql数据库全局序列号(cjw-2019-7-6)

- [x] JDBC连接池(jdbc模块)(cjw-2019-7-17)

- [x] JDBC连接池负载均衡(cjw-2019-7-19)

- [x] JDBC连接事务状态管理(cjw-2019-7-20)

- [x] JDBC连接池配置(cjw-2019-7-21)

- [x] JDBC MySQL集群心跳(cjw-2019-7-22)

- [x] Proxy读写分离专用路由(cjw-2019-7-24,Deprecated)

- [x] JDBC连接池单节点路由（cjw-2019-7-26,Deprecated）

- [x] JDBC结果集值转换成MySQL文本结果集（cjw-2019-7-28）

- [x] replca.yaml中mysqls的名字改为datasources（cjw-2019-7-29）

- [x] Atomikos分布式事务支持（cjw-2019-8-16）

- [x] proxy与jdbc集群/心跳处理类合拼（cjw-2019-8-16）

- [x] UTF8编码中跳过非ASCII编码工具（cjw-2019-9-4）

- [x] 分布式查询工具(Weiqing Xu,ChenJunwen-2019-9-9)

- [x] 动态注解(草稿)整合(cjw-2019-10-24)

- [x] HBT DSL完成(cjw-2019-11-27)

- [x] DSL节点变换器试验(cjw-2019-12-20)

- [x] 在新建的连接中使用负载均衡算法(proxy模块)

- [x] proxy内实现响应流量写入本地文件(mycat2模块)

- [x] proxy内实现本地文件作为响应(mycat2模块)

  

##### 2019年未完成

- [ ] proxy内实现SQL限流的功能(mycat2模块)
- [ ] proxy内本地文件全局序列号(router模块)
- [ ] 结果集对象池(common模块)
- [ ] 路由提供HTTP的测试接口(router模块)
- [ ] mycat2配置集群配置同步(新模块)
- [ ] change user 命令
- [ ] 报文生成工具优化
- [ ] 更多的全局序列号生成算法

------



