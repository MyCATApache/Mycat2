#### 任务列表

1. 参与重要模块的五位同学赋予项目权限,无需走pull request流程,三周无法给出关键代码取消权限

2. 没有权限的同学请走pull request流程

3. 如果代码功能完成并有相应的修改说明,就可以记录到此文件,并留下指向github账户的链接和完成日期

4. 开发环境使用以JDK8为最低版本,但是仅使用JDK8 API开发

5. 测试环境

   服务器10.3.15-MariaDB对应MySQL5.7

   客户端:mysql-connector-java-8.0.11,mariadb-java-client 2.5.2



##### 进行中
HBT功能相关



##### 拦截器匹配需求收集

一种针对sql的匹配器,主要是mysql,但完备情况上不限sql方言
1.支持sql词法,主要是mysql
2.支持多语句,以;分隔的多个sql的字符串,匹配模式中的;原则上不影响多语句中每个sql的匹配,如果不支持这个特性,那意味着每个匹配模式都要写两个版本,一个带;一个不带
3.一个匹配模式对应一个sql
4.支持通配符 {xx}是字符串捕获,在匹配模式中select id from {xxxx} 可以对应select id from travelrecord 或者 select id from travelrecord where id = 1;
此时xxx是travelrecord 或者travelrecord where id = 1
5.支持?词法单元捕获,参考预处理的匹配模式,此单元原则上就是mysql词法单元的一个字面量,对应数值类型,字符等,而不是1+1(表达式)这类的非字面量
6.支持匹配条件,比如 id = 1 or user = 1或者 id = 1 and user = 1可以提供一种模式语法提取逻辑运算符,例如 id = 1 #{condition } user = 1
7.支持按表名分组匹配,提取表名
8.支持按字段分组匹配
9.高性能,sql的长度可能是很长的,对于一些常用sql可以瞬速预判,然后走特定的匹配场景
10.输出结果应该是带有原sql的下标的,方便改写
11.支持byte基本的解析,无需转成String再匹配,byte是按照utf8编码的
12.支持中文
13.sql注释可以开启可以提取也是很重要
14.动态更新
15.检查到匹配模式有冲突在启动时候给与提示
上述功能不要求全部实现,尽可能实现即可



------



##### 2020.3.16 -> 2020.3.23开发计划

划2020-3-23 HBT语言对外公布
1.完善HBT基本测试例子
2.输出HBT文档



##### 2020.3.9 -> 2020.3.15开发日志
https://github.com/MyCATApache/Mycat2/commit/86df5c18bb5bfeb2608c3f952175f6dcb93371dd

1.HBT能提供对实际执行计划输出功能
其中explain输出底层的迭代器的字段类型,用于调试时候能看到每个语法节点的类型
https://github.com/MyCATApache/Mycat2/commit/cc3ec77b1b075c4616bd096362370b8c3530fec8
https://github.com/MyCATApache/Mycat2/commit/7c0349a97a71dfcd12134713ec3ab9daf9ceefd3

2.重构mycat 2.0 NIO框架以便支持3种线程事务模式(事务与线程绑定,非绑定,reactor),为支持更多事务框架做支持,两种写入模式(透传,自定义报文写入)
修改原因:

使用不同的技术实现HBT功能后发现mycat内部的状态过于分散,难以维护

mycat2.0使用多种SQL以及事务处理工具完成像数据库一样的功能,每种工具有它们特殊的执行环境,它们之间有类似的变量,
在mycat2.0以proxy为中心+插件(jdbc等模块)的架构下,变量的修改,以及线程之间的交互,报文写入的代码分散,若不进行重构,将难以维护.
完成此修改后,架构变更为以事务处理接口(该接口是外部提供)为中心,统一的前端接收报文,以可选的透传结果集和自定义报文写入响应的架构.
统一提供一个事务处理接口,该接口提供事务处理的方式.将来可能对自定义报文写入提供缓存写入的功能,由于这个修改不处于计划之内,所以暂未实现.

该重构通过基本测试,但可能存在一些未处理的情况,这将是一个长期重构计划

https://github.com/MyCATApache/Mycat2/commit/f3189bb071cb7514695b357b72c5e49e0d3b130f

3.添加前端异常连接检测,IO超时检查(辅助测试,辅助释放资源)
https://github.com/MyCATApache/Mycat2/commit/78b6103b4497c0a1ea7f3922ebe1a7dab1fd80d8

4.@wangzihaogithub提交简单的数据源读取写入代码
https://github.com/MyCATApache/Mycat2/commit/51b98bf63428e0341827f017db045fb0afccc30d

待完善任务
1.绑定事务的线程池优化,浪费一个监控线程维护等待的事务
2.绑定事务线程与多线程报文写入方式优雅实现
3.完善自定义报文缓存写入,已有原型,未整合





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



