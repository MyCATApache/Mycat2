# mycat 2.0-readme

author:junwen  2020-1-10

联系: qq:  294712221

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

项目地址:<https://github.com/MyCATApache/Mycat2>

## 特点

1.proxy透传报文,使用buffer大小与结果集无关

2.proxy透传事务,支持XA事务,jdbc本地事务

3.支持分布式查询

## 限制

暂不支持MySQL压缩协议,预处理,游标等(实验状态)



测试版本的mycat2无需账户密码即可登录

``

## 表



#### 分片类型

##### 自然分片

单列或者多列的值映射单值,**分片算法**使用该值计算数据节点范围

##### 动态分片

单列或者多列的值在分片算法计算下通过**关联关系**映射分片目标,目标库,目标表,得到有效的数据节点范围



#### 存储节点


dataNode是数据节点,库名,表名组成的三元组

targetName是目标名字,它可以是数据源的名字或者集群的名字

分片必然分库
分库必然分表


| 目标 targetName| 库schemaName   | 表tableName   | 类型   |
| ---- | ---- | ---- | ------ |
| 唯一 | 唯一 | 唯一 | 非分片 |
| 唯一 | 多目标 | 相同名字 |   非分片分库分表     |
|  唯一    |  唯一    |  不同名字    |   非分片单库分表     |
| 跨实例 | 多目标 | 不同名字 | 分片分库分表 |

在跨实例情况下,计算要聚合,事务要协调



## 事务

XA事务使用基于JDBC数据源实现,具体请参考Java Transaction API

Proxy事务即通过Proxy操作MySQL进行事务操作,本质上与直接操作MySQL没有差异.

为了方便上层逻辑操作事务,所以统一JDBC和Proxy操作,,参考JDBC的接口,定下mycat2的事务接口



### commit
