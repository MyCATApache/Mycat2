# mycat 2.0 proxy ReadAndWriteSeparationHandler

author:junwen 2019-7-29

本文描述在2019年7月29号后mycat router的一些行为

对于mysql的命令处理分为三类处理器

## 读写分离（ReadAndWriteSeparationHandler）

该处理类的特点

**拦截**以下SET SESSION TRANSACTION语句

```sql
SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

并记录隔离级别

READ_UNCOMMITTED

READ_COMMITTED

REPEATED_READ

SERIALIZABLE



**拦截**以下SET AUTOCOMMMIT语句

```sql
SET autocommit = 1;
SET autocommit = 0;
SET autocommit on;
SET autocommit off;
```

并记录autocommit状态





##### 路由到读节点的SQL满足以下条件

不在事务状态下，不是select for update ，不是select into 的查询语句

**其他SQL统一路由到主节点**



##### 事务处理

对于存在事务的状态，前端会话会与后端会话绑定，mycat注解失效，直到事务消失

在没有事务状态的情况，sql注解生效



##### 支持但是不推荐使用的功能

预处理，游标(服务端预处理)，loaddata路由到主节点，但是不推荐对proxy使用这些功能



##### 与其他处理器对比不同的地方

对所有SQL都支持

支持多语句

超级简单的路由处理

mycat注解仅支持负载均衡和runOnMaster

不支持切换schema



##### 不支持的功能

不支持change user命令

不支持切换schema即没有实现use schema 和init db命令

此处理器不能用于jdbc的后端



下面仅列出关键的配置更改点

mycat.yaml

```yaml
proxy:
  proxyBeanProviders: io.mycat.command.ReadAndWriteSeparationHandler
```

  schema.yaml

```yaml
defaultSchemaName: 读写分离
schemas:
  - name: DB_IN_ONE_SERVER_3306
    defaultDataNode: dn1
```



```yaml
replicas:
  - name: repli                      # 
    repType: MASTER_SLAVE           # 复制类型 读写分离必须配置该属性
    switchType: NOT_SWITCH              # 切换类型, 读写分离按情况配置该属性
    balanceName: BalanceRoundRobin   # 负载均衡算法名字
    balanceType: BALANCE_ALL 
    datasources:
      - name: mytest3306b              # mysql 主机名
        ip: 127.0.0.1               # i
        port: 3306                  # port
        user: root                  # 用户名
        password: 123456      # 密码
        minCon: 1                   # 最小连接
        maxCon: 1000                  # 最大连接
        maxRetryCount: 3            # 连接重试次数
        weight: 3            # 权重
        initDb: db1			#读写分离必须配置该属性
        slaveThreshold: 0   #主从节点同步延迟阈值,默认为0
```

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)

This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------

