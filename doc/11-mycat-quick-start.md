# mycat 2.0 quick start(快速开始)

author:junwen 2019-6-3

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

## 前提信息

1. 用于登录mycat的用户名,密码

2. 逻辑库的名称,物理表的名称

3. 数据库架构类型

   负载均衡(例如读写分离)

   在逻辑库聚合多个mysql服务器的物理表

   表数据分片(请看配置指南)

## 配置步骤

### 步骤1

修改user.yaml中的用户名,密码,逻辑库的名字

```yaml
users:
  - name: root
    password: 123456
    schemas:
      - test
```

### 步骤2

修改replica.yaml的数据库连接信息并记录复制组的名称供外部引用

```yaml
replicas:
  - name: repli                      # 复制组 名称   必须唯一
    repType: SINGLE_NODE           # 复制类型
    switchType: SWITCH              # 切换类型
    balanceName: BalanceLeastActive   #负载均衡算法名称
    balanceType: BALANCE_ALL #负载均衡类型
    mysqls:
      - name: mytest3306              # mysql 主机名
        ip: 127.0.0.1               # i
        port: 3306                  # port
        user: root                  # 用户名
        password: 123      # 密码
        minCon: 1                   # 最小连接
        maxCon: 1000                  # 最大连接
        weight: 3            # 权重
```

### 步骤3

修改schema.yaml,以下两个个架构选一个配置

#### 负载均衡

schemas - name是逻辑库的名称

dataNode的database是mysql物理库的名称

replica是上述的复制组的名字

```yaml
schemas:
  - name: db1
    schemaType: DB_IN_ONE_SERVER
    defaultDataNode: dn1
    tables:

dataNodes:
  - name: dn1
    database: db1
    replica: repli
```

#### 在逻辑库聚合多个mysql服务器的物理表

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





------

