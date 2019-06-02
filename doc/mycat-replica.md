## mycat 2.0-replica

author:junwen,zhangwy 2019-6-2

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 概念

## 副本(replica)

副本对应mycat 1.6的节点主机概念

副本是一个数据库集群的抽象,该数据库集群尽可能提供对外一致性,高可用,数据同步等特性

## 副本类型(repType)

## mysql数据源

mycat proxy是自研实现的mysql proxy代理,具备mysql 服务器,客户端,代理三种形态的能力.

mysql数据源是mycat使用自研的mysql客户端进行通信

## JDBC数据源

使用JDBC作为数据源连接(暂不支持)

## 前提

副本配置依赖负载均衡配置

负载均衡配置在插件模块(plug)在插件模块(plug)



```yaml
replicas:
  - name: repli                      # 复制组 名称   必须唯一
    repType: SINGLE_NODE           # 复制类型
    switchType: SWITCH              # 切换类型
    balanceName: BalanceLeastActive   # 读写分离类型
    balanceType: BALANCE_ALL  #balance 0 不开启读写分离机制 2;  所有读操作都随机的在 writeHost、readhost 上分发; 3 所有读请求随机的分发到 wiriterHost 对应的 readhost 执行，writerHost 不负担读压力
    mysqls:
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
```