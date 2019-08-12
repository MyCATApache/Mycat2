

## mycat 2.0-replica(replicas.yml,masterIndexes.yml,复制组,mysql集群)

author:junwen,zhangwy 2019-6-2

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 概念

## 复制组(replica)

复制组对应mycat 1.6的节点主机概念

复制组是一个数据库集群的抽象,该数据库集群尽可能提供对外一致性,高可用,数据同步等特性

## 复制组类型(repType)

单一节点

普通主从

普通基于garela cluster集群

## 切换类型

当主节点无法通行的时候是否根据masterIndexes中的配置进行主从切换

## mysql数据源

mycat proxy是自研实现的mysql proxy代理,具备mysql 服务器,客户端,代理三种形态的能力.

mysql数据源是mycat使用自研的mysql客户端进行通信

## JDBC数据源

使用JDBC作为数据源连接(0.2开始支持)

## 前提

复制组配置依赖负载均衡配置

负载均衡配置在插件模块(plug)在插件模块(plug)

## 复制组配置(replicas.yml)

```yaml
replicas:
  - name: repli                      # 复制组 名称   必须唯一
    repType: SINGLE_NODE           # 复制类型
    switchType: SWITCH              # 切换类型
    readBalanceName: BalanceRoundRobin   # 负载均衡算法名字
    balanceType: BALANCE_ALL #负载均衡类型 BALANCE_ALL BALANCE_ALL_READ  BALANCE_NONE
    datasources:
      - name: mytest3306              # mysql 主机名
        ip: 127.0.0.1               # i
        port: 3306                  # port
        user: root                  # 用户名
        password: 123      # 密码
        minCon: 1                   # 最小连接
        maxCon: 1000                  # 最大连接
        maxRetryCount: 3            # 连接重试次数
        weight: 3            # 权重
      - name: mytest3340              # mysql 主机名
        ip: 127.0.0.1               # i
        port: 3340                  # port
        user: root                  # 用户名
        password: 123      # 密码
        minCon: 1                   # 最小连接
        maxCon: 1000                  # 最大连接
        maxRetryCount: 3            # 连接重试次数
        weight: 1            # 权重
        initSQL: select 1; #该属性一般不写,作用是创建连接后马上执行一段初始化SQL,支持多语句
        initDb: db1 #后端连接数据库的初始化database
        slaveThreshold: 0 #主从延迟阈值,在repType:MASTER_SLAVE下生效
```

### 复制组属性

```yaml
replicas:
  - name: repli                      # 复制组 名称   必须唯一
    repType: SINGLE_NODE           # 复制类型
    switchType: SWITCH              # 切换类型
  	readBalanceName: BalanceRoundRobin   # 负载均衡算法名字
    balanceType: BALANCE_ALL #负载均衡类型 BALANCE_ALL BALANCE_ALL_READ  BALANCE_NONE
```

#### name

复制组的名称,必须唯一,供外部配置引用

#### repType(复制组的类型)

##### 单一节点

```
SINGLE_NODE
```

##### 普通主从

```
MASTER_SLAVE
```

##### 普通基于garela cluster集群

```
GARELA_CLUSTER
```

#### switchType(切换类型)

不进行主从切换

```yaml
NOT_SWITCH
```

进行主从切换

```yaml
SWITCH
```

#### readBalanceName

负载均衡算法的名称,引用(plug)插件配置的负载均衡算法,用于选择节点的算法

#### balanceType

负载均衡算法的类型

所有数据源参与负载均衡

```
BALANCE_ALL
```

所以非master数据源参与负载均衡

```
BALANCE_ALL_READ
```

只有master(一个)参与负载

```
BALANCE_NONE
```

### 数据源属性

```yaml
  datasources:
      - name: mytest3306              # mysql 主机名
        ip: 127.0.0.1               # i
        port: 3306                  # port
        user: root                  # 用户名
        password: 123      # 密码
        minCon: 1                   # 最小连接
        maxCon: 1000                  # 最大连接
        weight: 3            # 权重
```

#### name

供外部引用,便于调试的数据源名字

#### ip

mysql 连接的ip

#### port

mysql连接的端口

#### user

mysql连接登录用户名

#### password

上述用户名的密码

#### minCon

初始化该数据源的创建的连接数量,保持的最小连接数

#### maxCon

数据源连接最大的限制连接数量

#### weight

权重

#### initSQL

 该属性一般不写,作用是创建连接后马上执行一段初始化SQL,支持多语句

#### initDb

 后端连接数据库的初始化的database

#### slaveThreshold

主从延迟阈值,在repType:MASTER_SLAVE下生效

#### dbType

数据源的类型,一个集群的数据源的类型是一致的

当dbType中有mysql字符串或者不设置该属性的时候,proxy创建此数据源配置的连接

#### url

jdbc连接的url,当设置该属性的时候,会使用jdbc创建连接,jdbc的连接,集群管理是与proxy的连接,集群管理是互不影响独立的.

## 数据源主节点下标记录(masterIndexes.yml)

```yaml
masterIndexes:
  repli: 0
  repli2: 0
```

repli是复制组名字

0是主节点的下标

在对应的集群是GARELA_CLUSTER才可以配置多个下标,SINGLE_NODE和MASTER_SLAVE只能配置一个下标

```yaml
masterIndexes:
  repli: 0,1 #GARELA_CLUSTER
  repli2: 0
```



在集群是SINGLE_NODE和MASTER_SLAVE类型下

当发生主从切换的时候,主节点的下标会改变并更新该文件

但是在GARELA_CLUSTER下,并不会更新该文件,仅仅是不再把请求发送到无法访问的主节点



主从切换记录也会作为日志记录在mycat-replica-indexes.log文件之中



------

