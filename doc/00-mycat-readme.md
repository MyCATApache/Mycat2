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

[代理配置(mycat.yml)](01-mycat-proxy.md)

[用户配置(user.yml)](02-mycat-user.md)

[mysql集群配置(replicas.yml)](03-mycat-replica.md)

[逻辑库配置(schema.yml)](04-mycat-schema.md)

[路由规则配置(rule.yml)](05-mycat-dynamic-annotation.md)

[分片算法配置(function.yml)](06-mycat-function.md)

[心跳配置(heartbeat.yml)](07-mycat-heartbeat.md)

[静态注解说明](08-mycat-static-annotation.md)

[插件说明(plug.yaml](09-mycat-plug.md)

[打包](10-mycat-package.md)

## [待办事项与开发历史记录](101-todo-history-list.md)

## 开发技术列表

[mysql-packet-parsing-state-machine](100-mysql-packet-parsing-state-machine.md)



## [文档编辑指南](99-edit-guide.md)

------

