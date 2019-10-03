# 动态注解指令

author:junwen 2019-10-2



*注解指令函数api并不稳定

本文仅描述API设计意图



在注解指令的系统中,必须返回一个可执行的对象,该对象实现执行的函数



## ProxyExport

该库提供MySQL代理的基本函数

```
Response useSchemaThenResponseOk(String schema)
```

切换schema,该函数保存的状态会影响SQL解析器,所以在该库中提供是必要的



```
Response responseOk()
```

返回Ok包,Ok包的状态在session里面设置



## FinalSQLResultSetExport

前提:内容不会被改变的SQL文件

目标:代理可以返回以SQL文件保存的结果集

具体:

在代理启动的时候加载保存在本地的SQL文件,转换该SQL文件变成结果集.注解请求该文件的路径的时候,返回该文件对应的结果集

该功能移除已经加载的结果集



#### 初始化函数

##### 初始化缓存文件

```
void initFinalSQLCacheFile(String fileName)
```

fileName是保存多个SQL文件构建多个结果集的缓存文件,一旦SQL文件转成结果集后,就不会再使用文件构建结果集

##### 加载结果集

```
void finalSQLFile(String fileName)
```

把SQL文件加载到缓存文件



#### 响应结果集函数

```
Response responseFinalSQL(String fileName)
```

把SQL文件名对应的结果集响应执行对象返回



#### SQL文件格式

```SQL
create table `SCHEMATA` (
   `Database` varchar (256)
);
insert into `SCHEMATA` (`Database`) values('TESTDB1');
insert into `SCHEMATA` (`Database`) values('TESTDB2');
```

创建表语句会转换成结果集的字段数据

插入语句会转换成每一行数据

每个插入语句不能跨行,一行一条



## CacheResultSetExport

目标:

对响应的一个结果集的迭代器对象的字段数据和行数据缓存

对于已经缓存的结果集,不会触发对结果集的求值

支持删除已经缓存的结果集

具体:

一个结果集缓存对应一个本地文件

对结果集缓存就是创建本地文件

删除结果集就是删除本地文件

缓存的键可以是文件名



创建结果集缓存

```java
MycatResultSetResponse cacheResponse(String key, Supplier<MycatResultSetResponse> supplier)
```

Supplier代表一个求值过程的类型

MycatResultSetResponse是结果集迭代器



```
void removeCache(String key)
```

移除缓存



## JdbcExport

```java
beginOnJdbc
```

使用JDBC模块进行事务开启



```java
commitOnJdbc
```

使用JDBC模块进行事务提交



```
rollbackOnJdbc
```

使用JDBC模块进行事务回滚



```
public static Response setTransactionIsolation(String text)
```

设置事务隔离级别并返回响应



```
Response responseQueryOnJdbcByDataSource(String dataSource, String sql) 
Response responseQueryOnJdbcByDataSource(String dataSource, String... sql) 
```

查询方法族

查询并返回一个结果集响应



```java
Response updateJdbcByDataSource(String dataSource, boolean needGeneratedKeys, String... sql) 
```

执行更新操作的SQL



```java
public static Response setAutocommit(boolean autocommit)
```

设置自动提交



## CalciteExport

```
 Response responseQueryCalcite(String sql)
```

使用Calcite进行分布式查询

依赖28-mycat-sharding-query.md



[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)

This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------



