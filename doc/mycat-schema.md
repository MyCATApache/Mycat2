# mycat 2.0-schema(schema.yml,逻辑库)

author:junwen 2019-6-1

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## 前提

根据schema使用不同的SQL解析方式,以适应不同的应用,提高性能.

暂时不支持跨schema的SQL.

要求SQL中不带有schema

仅支持使用

```sql
use {schema}
```

或者initDb命令切换schema

### 概念

#### 逻辑库(schema)

在mycat中,一个逻辑库描述了逻辑表的组织方式,在mycat 2.0更进一步地,schema指定了SQL的路由方式.

#### 分片节点(dataNode)

分片节点一般指数据库分片节点,

一个数据库分片节点对应一个网络地址+端口+用户+指定的数据库schema的四元组

因为发往数据库分片节点的SQL不包含schema

所以如果数据范围不在一个数据库分片节点上,则可能需要多条SQL语句(多次请求)完成查询.

#### 逻辑表

在mycat中,一个逻辑表描述了数据的组织方式.

一般来说,逻辑表的数据组织方式一般有三种

##### 分库

逻辑表数据分布于不同的分片节点

分片节点上存在于逻辑表名一致的物理表

##### 分表

逻辑表数据分布于不同的物理表

所有的物理表在一个分片节点上

##### 分库分表(暂不支持)

逻辑表数据分布于不同的分片节点上不同的物理表

#### 物理表

数据库服务器上的表

#### 物理库

数据库服务上的schema



#### DB IN ONE SERVER

##### 描述

所有的表都在同一个mysql服务器上.

一个逻辑库就是一个分片节点,切换逻辑库就是切换分片节点.

逻辑表与该分片节点上的物理表完全对应

##### 限制

如果逻辑库的名字与物理库的名字不一致,则需要移除sql中的schema.

使用mycat proxy session中当前的逻辑库进行路由,所以可以达到对sql没有任何修改和路由限制.



#### **DB IN MULTI SERVER**

##### 描述

不同的表在不同的mysql服务器上

逻辑库与分片节点没有对应关系

一个逻辑表名对应一个分片节点

逻辑表完全对应该分片节点上的一个物理库

##### 限制

禁止SQL带有schema.

一般来说,一个SQL必须仅有一个table,才能正常路由

一个SQL中包含多个语句.如果他们的table都是一致的,则可以正常路由.

SQL中的逻辑表必须在mycat proxy session中的当前的逻辑库



#### 分片键模式

##### 描述

不同的表在不同的mysql服务器上

逻辑库与分片节点没有对应关系

分片节点的物理表组成逻辑表,逻辑表结构对应物理表结构

一个分片节点存放一个范围的分片数据

分片键对应dataNode

##### 限制

涉及多个分片的SQL要转换处理



以下是可能的实现

**ANNOTATION ROUTE**

##### 描述

该模式支持逻辑表分片

使用模式匹配提取用于计算分片节点的信息

- 分片值
- 范围分片值

使用两种类型的值作为分片算法的参数,计算出分片节点

##### 限制

参考mycat-dynamic-annotations.md



##### SQL PARSE ROUTE

暂时还没有实现,参考1.6的描述和限制



## 配置

### 分片节点配置

```yaml
dataNodes:
  - name: dn1
    database: db1
    replica: repli
```

name

分片节点的名字,其他配置可以引用该名字

replica

数据库集群的名字,引用replica的名字

database

物理数据库上的一个物理库的名字



## 逻辑库配置

```yaml
schemas:
  - name: db1
    schemaType: DB_IN_ONE_SERVER
    defaultDataNode: dn1
```

name

逻辑库的名字

schemaType

schema的类型

defaultDataNode

默认的分片节点

暂时只在DB_IN_ONE_SERVER上使用



## 逻辑表配置

name

除分表之外(暂不支持),逻辑表的名字,该名字与物理表的名字对应

dataNodes

引用dataNode配置的名字,以,分隔配置多个

在DB_IN_MULTI_SERVER模式下,只有第一个生效

在分片模式(ANNOTATION_ROUTE)模式下,分片节点的数量应该要符合相应的分片算法的配置

 type

暂时只有ANNOTATION_ROUTE需要指明是SHARING_DATABASE,因为还有全局表等类型(还不支持)



##### DB_IN_ONE_SERVER

```yaml
schemas:
  - name: db1
    schemaType: DB_IN_ONE_SERVER
    defaultDataNode: dn1
    tables:
      - name: travelrecord


dataNodes:
  - name: dn1
    database: db1
    replica: repli
```

在该模式下,tables实际上可以不配置,配置tables的用途是作为show databases的结果

##### DB_IN_MULTI_SERVER

```yaml
schemas:
  - name: db1
    schemaType: DB_IN_MULTI_SERVER
    tables:
      - name: travelrecord
        dataNodes: dn1
      - name: travelrecord2
        dataNodes: dn2


dataNodes:
  - name: dn1
    database: db1
    replica: repli
  - name: dn2
    database: db2
    replica: repli
```

##### ANNOTATION_ROUTE

```yaml
schemas:
  - name: db1
    schemaType: ANNOTATION_ROUTE
    tables:
      - name: travelrecord
        dataNodes: dn1,dn2,dn3,dn4
        type: SHARING_DATABASE
dataNodes:
  - name: dn1
    database: db1
    replica: repli
  - name: dn2
    database: db2
    replica: repli
  - name: dn3
    database: db3
    replica: repli
  - name: dn4
    database: db4
    replica: repli
```





