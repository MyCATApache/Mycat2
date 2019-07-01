# mycat 2.0 proxy sql

author:junwen 2019-6-14,7-1



该文档描述的内容涉及代理中mysql session mycat session

以下的sql是根据jdbc的内部发出的sql整理而成.

在mycat的mysql session是可以被多个mycat session重复使用.

而一个客户端连接对应一个mycat session,mycat session保存了客户端设置的变量.mycat作为代理需要把这些属性与进行代理的mysqlsession状态对应,所以mysql session设置mycat保存的变量的过程,称之为同步.才可以发送真正的SQL.因为proxy不进行复杂的SQL解析处理,所以仅处理以下SQL.



##### schema

```sql
use {schema}
```

该sql切换mycat session当前的schema



##### 同步属性autocommit

```sql
set autocommit = 1;
set autocommit = 0;
set autocommit on;
set autocommit off;
```

该sql切换mycat session当前的autocommit状态



##### 同步属性charset

```sql
set names {charset}
```

该sql切换mycat session当前的charset变量



##### 同步属性character_set_results

```sql
SET character_set_results {charset}
```

该sql切换mycat session当前的character_set_results变量



```sql
SET  GLOBAL TRANSACTION xxx
```

GLOBAL级别 返回错误



##### 同步属性asscess mode

```
set session transaction {read only|read write}
```

记录access_mode,并返回ok



##### 同步属性isolation

```
SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

记录隔离级别,并返回ok



##### 同步属性SET_SQL_SELECT_LIMIT

```sql
SET SQL_SELECT_LIMIT= {dafault|数字}
```

记录access_mode,并返回ok



##### 同步属性net_write_timeout

```sql
SET net_write_timeout= {dafault|数字}
```

记录net_write_timeout,并返回ok



##### show tables

```sql
show tables
```

返回mycat当前schema的所有逻辑表名字



##### describe与show语句

转发到当前schema的defaultDataNode



##### show variables

```sql
show variables
```

返回variables.yaml配置文件的内容



##### show warnings

```sql
show warnings
```

发送mycat会话中的lastMessage信息,一般是空的



##### session变量

```sql
select @@session.transaction_isolation;

select @@session.tx_isolation;

select @@session.autocommit;

select @@session.transaction_read_only;

select @@session.tx_read_only;
```

路由到一个随机mysql session即可

查询结果是是mysql服务器发出的,因为获取mysql session后会进行同步,所以结果是正确的

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).