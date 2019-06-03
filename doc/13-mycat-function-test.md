# mycat 2.0 function test(功能测试)

author:junwen 2019-6-3

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).



## 常量

```bash
MAX_PACKET_SIZE = 256 * 256 * 256 - 1
```

(schema处于DB_IN_ONE_SERVER模式下)

## 登录退出测试

- [ ] mysql-connector-java-8.0.16能登录mycat并获取连接
- [ ] mysql-connector-java-8.0.16关闭连接能关闭mycat对应的session
- [ ] mycat能创建mysql8或者mariadb连接



## 请求测试

- [ ] 客户端发送长度小于MAX_PACKET_SIZE-1的SQL语句,mycat能正常返回响应

- [ ] 客户端发送长度大于MAX_PACKET_SIZE-1但小于2(MAX_PACKET_SIZE-1)的SQL语句,mycat能正常返回响应

- [ ] 客户端发送长度大于2MAX_PACKET_SIZE但小于3MAX_PACKET_SIZE的SQL语句,mycat能正常返回响应

  

## 响应测试

- [ ] 客户端获取多行结果集,其中一行数据的长度小于MAX_PACKET_SIZE,mycat能正常返回响应.
- [ ] 客户端获取多行结果集,其中一行数据的长度大于MAX_PACKET_SIZE小于2MAX_PACKET_SIZE,mycat能正常返回响应.
- [ ] 客户端获取多行结果集,其中一行数据的长度大于2MAX_PACKET_SIZE小于3MAX_PACKET_SIZE,mycat能正常返回响应.



## 多结果集测试

- [ ] 客户端获取多结果集,mycat能正常返回响应.



## 客户端状态后端同步测试

- [ ] 获取连接前,jdbc设置字符集,mycat能在获取连接后把字符集设置SQL发送给后端连接
- [ ] 获取连接前,jdbc自动设置的结果集字符集,mycat能在获取连接后把结果集字符集设置SQL发送给后端连接
- [ ] 事务开始前,客户端设置session事务等级,mycat能把事务等级SQL发送给后端连接
- [ ] 在DB_IN_ONE_SERVER模式下切换schema,mycat能根据schema对应的DataNode,获取dataNode上当前设置的database,即同步schema



## 事务测试

- [ ] 客户端设置autocommit为true,mycat对于每次更新语句都可以解绑后端连接
- [ ] 客户端设置autocommit为false,除非提交事务或者回滚,mycat都不能解绑后端连接



## 资源占用测试

资源释放:每次响应结束,能观察到buffer释放日志,与分配日志对齐,数量一致

客户端获取多行结果集,其中一行数据的长度大于MAX_PACKET_SIZE小于2MAX_PACKET_SIZE,mycat能正常返回响应.而且内存使用没有变化.



## 异常测试

在mycat获取后端连接的时候,无法获取,mycat能向客户端发送错误报文

在带有事务状态的jdbc连接关闭,mycat能关闭前端和后端连接

在接收客户端数据的时候,客户端连接关闭,mycat能关闭前端连接和后端连接(如果有)

在向后端连接的写入数据时候,后端连接关闭,mycatt能后端连接,向客户端发送错误报文

在接收后端连接的数据时候,连接关闭,mycatt能关闭前端和后端连接

在向客户端写入数据的时候,客户端连接关闭,mycatt能关闭前端和后端连接(如果有)



## mysql常用语句测试

show databases

能返回schema配置的逻辑库

show tables

能返回schema配置的逻辑表

use schema

能切换schema

show variables

能返回variables表信息

show warnings

能返回错误信息









