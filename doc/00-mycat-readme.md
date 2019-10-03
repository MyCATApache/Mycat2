# mycat 2.0-readme

author:junwen,zhangwy 2019-6-2

author:junwen 2019-7-4

author:junwen 2019-7-30

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

项目地址:<https://github.com/MyCATApache/Mycat2>

## 优势

为单节点路由而优化的结果集响应透传

## 限制

- 暂不支持MySQL压缩协议
- 有限的SQL路由支持
- 暂不支持跨节点修改SQL和查询SQL(计划中)



客户端JDBC推荐连接字符串

```
jdbc:mysql://localhost:8066/TESTDB?useServerPrepStmts=false&useCursorFetch=false&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8
```

2019-7-30,jdbc作为数据源处于测试阶段

2019-9-9,分布式查询模块处于测试阶段

mycat.yml

commandDispatcherClass: io.mycat.grid.BlockProxyCommandHandler

启动jdbc路由,此时sql不会被路由到proxy模块的数据源,jdbc数据源默认配置支持分布式事务

该路由不会把mysql语句改写成数据源目标SQL



## 配置说明

[快速开始](11-mycat-quick-start.md)

[功能测试](13-mycat-function-test.md)

[代理配置(mycat.yml)](01-mycat-proxy.md)

[用户配置(user.yml)](02-mycat-user.md)

[mysql集群配置(replicas.yml)](03-mycat-replica.md)

[逻辑库配置(schema.yml)](04-mycat-schema.md)

[路由行为说明](20-mycat-router.md)

[JDBC内部SQL处理说明](18-proxy-sql.md)

[路由规则配置(rule.yml)](05-mycat-dynamic-annotation.md)

[分片算法配置(function.yml)](06-mycat-function.md)

[心跳配置(heartbeat.yml)](07-mycat-heartbeat.md)

[插件配置(plug.yaml](09-mycat-plug.md)

[日志](19-mycat-log.md)

[静态注解说明](08-mycat-static-annotation.md)

[分片算法说明](17-partitioning-algorithm.md)

[负载均衡说明](15-mycat-balance.md)

[负载均衡算法](16-load-balancing-algorithm.md)

[全局序列号说明](14-mycat-sequence.md)

[分布式查询](28-mycat-sharding-query.md)

[注解路由](30-mycat-dynamic-annotation.md)

[注解模式](29-mycat-gpattern.md)

[注解指令](31-mycat-instructions.md)

[打包](10-mycat-package.md)

[合作者](12-collaborators.md)

## [待办事项与开发历史记录](101-todo-history-list.md)



## [文档编辑指南](99-edit-guide.md)

------

