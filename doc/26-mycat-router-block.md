# mycat 2.0 proxy router block

author:junwen 2019-7-29

io.mycat.grid.BlockProxyCommandHandler



该处理器要完成以下功能

屏蔽MySQL的客户端适配细节

拦截以下语句并处理

##### 事务开启关闭语句(以下面token开始的SQL)

```sql
begin
start
start transcation 
commit
rollback
```

##### 设置 autocommit

`set autocommit`



##### 设置隔离级别

`set transaction`



##### 显示schema.yaml设置的表

show table 



##### 显示schema.yaml设置的schema

show databases



##### 显示variables.yaml的内容

show variables 



##### 显示空的警告结果

show warnnings



##### 切换schema

use 语句



##### 交给路由处理

update语句

delete语句

insert语句

select语句



##### 其他语句

返回空的结果集



##### 特点

由于mycat对jdbc数据源的封装没有实现多语句的支持,所以该路由并不支持多语句

因为是jdbc实现的后端数据源,所以理论上能使用jdbc作为后端连接的数据库的响应会根据jdbc规范转成mysql结果集响应

不支持SAVEPOINT 

支持多个节点提交事务,但是不是分布式事务实现

该处理器使用的路由配置与io.mycat.command.HybridProxyCommandHandler的一致,即20-mycat-router.md

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)

This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------

