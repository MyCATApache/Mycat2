# mycat 2.0 proxy router

author:junwen 2019-7-2,2019-7-3

本文描述proxy router的一些行为,用户需要知道这些行为适配满足业务系统

一些常见问题也需要看看jdbc的处理

[jdbc内部语句](18-proxy-sql.md)

从通讯协议上来说,mycat2都是支持的,难点在于路由,mycat proxy仅仅是路由,不会对结果集进行处理,跨节点的处理都不进行

MySQL语句处理可以分为三类

1. 文本语句

2. 预处理

3. loaddata local infle




## 文本语句的路由

代理中SQL路由需要从SQL以及代理的配置分析出以下信息,这是路由最终应该输出的参数

### 路由的最终输出

#### 选择数据分片

1. 发送到mysql服务器的SQL

   该SQL可能是原SQL也可能是修改后的SQL

2. 数据分片的名字

   数据分片的作用是指示要访问的集群与设置mysql连接当前的schema

   



#### 选择数据源

1. 是否把SQL发送到集群中的主节点
2. 负载均衡算法
3. 用于选择特定Mysql session的标识



##### 读写分离

路由使用该判断方式 非SELECT FOR UPDATE , SELECT INTO 语句路由到mysql集群的主数据源



##### 负载均衡算法

BALANCE_NONE:路由到主数据源

BALANCE_ALL_READ:路由到集群上所有读数据源

BALANCE_ALL:路由到集群上所有数据源



mysql session的标识符(用于预处理语句ID是与客户端绑定的,如果不重新构造预处理语句,预处理执行命令中的一个语句应该与mysql session是对应的)



### 路由的逻辑

路由的输入(当前的schema,SQL,是否存在绑定的后端的mysqlsession)

暂定的路由流程(2019-7-3)



#### 第一阶段

移除sql语句中的schema(还没有实现该功能可开启关闭)

判断是否有绑定的后端mysql session,如果有则直接发往该mysql



#### 第二阶段

处理[jdbc内部语句](18-proxy-sql.md)



#### 第三阶段

进入路由处理



注解指定dataNode,则使用该dataNode作为路由结果

注解指定schema 覆盖 当前schema,使用该schema上的路由策略进行路由

如果当前的sql带有schema,则使用该schema覆盖当前的schema,并使用该schema上的路由策略进行路由



##### DB IN ONE SERVER

使用schema上的默认dataNode作为路由结果



##### DB IN MULTI SERVER

如果该sql上存在schema,报错

如果该sql上不存在表名,报错

检查sql上所有表名是否相同,如果是,则使用该表名对应的dataNode进行路由,否则报错



##### ANNOTATION ROUTE SERVER

如果该sql上的表名不是等于一个,报错

如果存在注解中有分片字段值,就使用注解中的值作为分片算法的参数,

否则使用正则表达式提取分片字段值和范围分片字段值,作为分片算法参数计算出唯一一个DataNode,作为路由结果



如果静态注解有指定负载均衡算法和是否运行在主数据源的信息,则对路由结果进行设置



## 预处理语句

预处理语句在jdbc客户端里有两种,客户端处理方式与服务器端处理方式.详细的配置方式请参考其他资料.客户端处理方式就是客户端使用文本语句发送预处理.这个对于代理来说,与普通的SQL语句处理没有区别.而在服务器处理方式中

客户端与mysql服务器的预处理语句交互流程

1. 客户端会发送预处理语句到服务器
2. 服务器返回标志预处理语句的id
3. 客户端执行预处理命令之前,如果预处理参数有blob类型的值,会把值发送到服务器(longData)
4. 客户端执行预处理命令
5. 服务器返回预处理语句结果集



### mycat 2预处理语句

#### mycat proxy对于预处理语句处理如下(已实现)

##### 客户端会发送预处理语句到proxy

proxy 根据预处理语句的SQL**路由**到mysql

proxy把mysql响应的预处理语句id与mysql session记录,并修改响应报文,把id改成proxy生成的唯一的id(mycat_stmt_id),形成该关系(mycat_stmt_id,[(id,mysql_session)],一个mycat_stmt_id可以对应一个或者多个(id,mysql_session)的映射关系.[(id,mysql_session)]是可用的预处理mysql session的列表



##### Blob赋值

客户端执行预处理命令之前,如果预处理参数有blob类型的值,会把值发送到mycat,mycat根据请求报文中的预处理鱼护id和参数id,把该值保存,并不会把请求发送到mysql



##### 客户端执行预处理命令

mycat根据请求报文中的预处理语句id,找到(mycat_stmt_id,[(id,mysql_session)],如果可用预处理列表中的找到没有被其他原因占用的mysql session,从连接池中获取后,使用它执行预处理执行命令,如果mysql session都被占用了,则从连接池中获取一个新的mysql session发送预处理语句,这样能获得一个新的可用的mysql session.



如果之前存在blob类型的值,则把之前的值往mysql session发送,设置该mysql session需要清理 blob 的值

最后把真正需要执行的预处理执行命令发送到mysql session,mycat把mysql session的响应转发到客户端.



当mysql session归还到连接池的时候,清理blob的值,实际上该操作不是必须的,只是为了保证mysql session接近无状态.为mysql的预处理语句id实际上可以被多个mycat session使用做功能预留.





#### mycat grid对于预处理语句处理如下(计划中)

##### 客户端会发送预处理语句到proxy

mycat对sql语句结合表的信息进行分析,构造预处理语句响应报文,该操作与后端mysql session没有关系

##### Blob赋值

mycat根据请求报文中的预处理的id和参数id,把该值保存

##### 客户端执行预处理命令

把执行命令的参数与预处理语句变成完整的sql,进行路由

然后有三种处理方式

1. 把预处理语句转成文本语句,进行**路由**,然后执行文本语句,发送到mysql,把mysql的文本结果集响应转成二进制结果集并发送到客户端
2. 执行mycat proxy的处理方式
3. 自定义响应,mycat此时作为mysql服务器响应,与后端mysql无关



总结:在现有proxy对预处理的功能受限于sql解析,导致对预处理语句路由的调用点不一样.



## 游标

游标的预处理语句部分与预处理语句处理是一致的,仅仅是响应结果集可以多次获取

客户端与mysql服务器的游标交互流程

1. 客户端会发送预处理语句到服务器
2. 服务器返回标志预处理语句的id
3. 客户端执行预处理命令之前,如果预处理参数有blob类型的值,会把值发送到服务器(longData)
4. 客户端执行预处理命令
5. 服务器返回预处理语句结果集
6. 客户端执行fetch命令,服务器返回(无字段报文)二进制结果集
7. 客户端执行fetch命令,服务器返回(无字段报文)二进制结果集,如此直到没有结果集



### mycat proxy对于游标处理如下(已实现)

在步骤5前,处理与mysql proxy的处理方式一致,但是在执行执行命令之后,mycat会检测到响应报文存在游标的状态,所以mysql session会与mycat session处于绑定的状态,并不会归还到连接池.

在步骤6开始,mycat proxy会把fetch命令发送到绑定的mysql session,并把响应转发到客户端,同时检查响应中的游标状态,如果游标状态消失,就会把mysql 连接归还到连接池,否则继续绑定



### mycat grid对于游标处理如下(计划中)

在部署6的时候使用自定义响应,汇总多个节点的结果





## loaddata语句

客户端与mysql服务器的预处理语句交互流程

1. 客户端会发送loaddata语句到服务器

2. 服务器想客户端发送loaddataRequest响应

3. 客户端依旧使用mysql协议,响应文件内容

4. 客户端响应文件内容结束,发送一个ok报文

5. 服务器向客户端响应ok/error报文表示交互结束

   

### mycat 2 loaddata语句

#### mycat proxy对于loaddata语句处理如下(已实现)

1. 客户端会发送预处理语句到mycat proxy,mycat把loadata语句执行路由分析,与普通的sql一样,**路由**到mysql
2. mycat接收到loaddataRequest响应,转发给客户端,同时mycat对报文进行分析,得知这是loaddata的响应,会把后端的mysql连接继续绑定,并不会把连接归还连接池.
3. 客户端响应文件内容,mycat会把文件内容完整接收,并不会发送到mysql
4. 客户端响应文件内容结束,发送一个ok报文,此时mycat会把文件内容发送到mysql,最后发送的ok报文或者error报文

#### mycat grid对于loaddata语句处理如下(计划中)

1. 客户端会发送预处理语句到mycat proxy,mycat把loadata语句执行路由分析,得出是对那个表进行操作,并响应loaddataRequest,这个操作与后端mysql无关
2. 客户端响应文件内容,mycat会把文件内容完整接收,并分析loaddata的内容构造对涉及的分片节点的sql操作语句
3. 客户端响应文件内容结束,发送一个ok报文,mycat在接收到ok报文之后,开始对每个数据分片发送sql,并构造loadata响应的ok报文或者错误报文,在每个数据分片操作结束之后,把最终的报文发往客户端

总结:在现有proxy对loaddata的处理基本是没有的,仅仅是路由转发












[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------

