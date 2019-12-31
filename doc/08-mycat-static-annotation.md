

# mycat 2.0-static-annotation(静态注解)

author:junwen 2019-6-1

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

以下部分内容选自mycat权威指南第一版

## 前提

注解支持的'!'不被 mysql 单库兼容，
注解支持的'#'不被 mybatis 兼容
新增加 mycat 字符前缀标志

```sql
/** mycat: */
```

从 1.6 开始支持三种注解方式：

```java
/*#mycat:xxx*/ select * from travelrecord
/*!mycat:xxx*/ select * from travelrecord
/**mycat:xxx*/ select * from travelrecord 
```



## 参数命名规范

- 为了统一,采用(小)驼峰命名法
- 注解词法解析不区分大小写
- 不对参照值进行要求



## 路由执行流程(暂时不支持预处理,跨分片查询,插入)

1. mysql客户端请求->报文类型分析->报文类型分发->普通查询语句报文处理

2. 解析注解参数,提取table,schema,分割语句

3. 使用路由解析

4. 根据路由结果类型进行处理获得发往的后端mysql的以下信息

   sql

   是否读写分离

   dataNode的名字

   负载均衡策略对象

   

   mycat提供默认的路由实现,

   可以根据注解或者配置进行更改路由实现,自行实现路由在单节点路由情况上必须提供上述参数作为路由结果



## mycat路由注解

mycat注解有多种类型

- 路由类型

- 数据源选择类型


一个类型只能配置一次,禁止配置多次

静态注解的参数会覆盖动态注解的参数



## 路由类型

### schema注解(多租户注解)

通过注解方式在配置多个 schema 情况下，指定走哪个配置的 schema。

在 db 前面建立 proxy 层，代理所有 web 过来的数据库请求。proxy 层是用 mycat 实现的，web 提交
的 sql 过来时在注释中指定 schema, proxy 层根据指定的 schema 转发 sql 请求。

```sql
/*! mycat:schema = test_01 */ sql ; 
```

web 可进行部分改进,减少手动添加注解的工作：

1. 在用户登录时，在线程变量（ThreadLocal）中记录租户的 id
2. 修改 jdbc 的实现：在提交 sql 时，从 ThreadLocal 中获取租户 id, 添加 sql 注释，把租户的 schema
   放到注释中。

当使用这个注解之后,mycat 连接当前的schema变成注解中的schema

```sql
/*! mycat:schema = test_01,sql=select * from table where id = 1*/ create table travelrecord(id int);
```

schema=后面的schema指定schema,并切换当前schema

sql=后面的sql是用于路由分析的路由,而非注释sql则是真正发送到mysql服务器执行sql

### 分片键注解

```sql
/*! mycat:shardingKey=1*/ create table travelrecord(id int);
```

shardingKey=后面的值作为分片算法的值

```sql
/*! mycat:shardingRangeKey=1,2*/ create table travelrecord(id int);
```

shardingKey=后面的两个值作为分片算法的范围查询的值

可以与schema注解一起使用

```sql
/*! mycat:schema = test_01,shardingKey=1*/ create table travelrecord(id int);
```

```sql
/*! mycat:schema = test_01,shardingRangeKey=1,2*/ create table travelrecord(id int);
```

### DataNode路由注解

```sql
/*! mycat:deafultDatabase=dn1*/ create table travelrecord(id int);
```

deafultDatabase=后面就是最终路由的节点,此sql不经过路由处理,而非注释sql则是真正发送到mysql服务器执行sql



## 数据源选择类型

### 选择主从注解

```sql
/*! mycat:runOnMaster = 1*/ select a.* from customer a where a.company_id=1; 
```

1.强制sql发往主节点

0,强制sql发往从节点

### 数据源负载均衡策略

```sql
/*! mycat:balance = 插件包中的负载均衡策略名字*/ select a.* from customer a where a.company_id=1; 
```

使用指定的负载均衡策略选择数据源

## 多种类型注解综合运用

```sql
/*! mycat:schema = test_01,shardingKey=1,runOnMaster = 1*/ select a.* from customer a where a.company_id=1; 
```

## 自定义路由注解

```sql
/*!_路由名字:参数1=值1,参数2=值2*/ create table travelrecord(id int);
```

mycat读取路由并使用用户提供的路由进行sql进行处理

## 注意事项

注解参数键值对最多8个

参数类型:标识符,整型数字,字符串(''包裹)

暂时多语句SQL最多一次解析256条



------

