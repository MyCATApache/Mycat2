# mycat 2.0-readme

author:junwen,zhangwy 2019-6-2

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 优势

为单节点路由而优化的结果集响应透传

## 限制

- 暂不支持MySQL压缩协议
- 暂不支持预处理,LOAD DATA infile
- 暂不支持跨节点修改SQL和查询SQL
- 有限的SQL路由支持

## 配置说明

[代理配置(mycat.yml)](mycat-proxy.md)

[用户配置(user.yml)](mycat-user.md)

[集群配置(replicas.yml)](mycat-replica.md)

[逻辑库配置(schema.yml)](mycat-schema.md)

[路由规则配置(rule.yml)](mycat-dynamic-annotation.md)

[分片算法配置(function.yml)](mycat-function.md)

[心跳配置(heartbeat.yml)](mycat-heartbeat.md)

[静态注解说明](mycat-static-annotation.md)




