# mycat 2.0 分布式查询

author:junwen 2019-9-8

2019-9-8提供分布式查询模块测试,仅支持通过mysql直连方式操作,不支持jdbc等连接方式



以下是测试的配置更改点,



##### mycat.yml

commandDispatcherClass是io.mycat.grid.CalciteCommandHandler

```yml
proxy:
  commandDispatcherClass: io.mycat.grid.CalciteCommandHandler
```

##### replica.yaml

配置jdbc连接数据源

```yaml
replicas:
  - name: repli                      # 复制组 名称   必须唯一
    datasources:
      - name: mytest3306a              # mysql 主机名
        ip: 127.0.0.1               # i
        port: 3306                  # port
        user: root                  # 用户名
        password: 123456      # 密码
        minCon: 1                   # 最小连接
        maxCon: 1000                  # 最大连接
        maxRetryCount: 3            # 连接重试次数
        weight: 3            # 权重
        dbType:   mysql-xa
        initDb: db1
        url: jdbc:mysql://localhost:3306/db1?useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai&pinGlobalTxToPhysicalConnection=true
 
```



##### function.yml

配置需要使用的分片算法

```yaml
functions:
  - clazz: io.mycat.router.mycat1xfunction.PartitionByLong
    name: partitionByLong
    properties:
      partitionCount: 2,1
      partitionLength: 256,512
```



##### shardingQuery.yml

配置分片关系(以下配置已过时,但可以参考它,更改新的配置文件)

```yaml
#必须带上!!类型提示
schemas:
#逻辑库名字
  TESTDB:
    #逻辑表名字,
    ADDRESS: !!io.mycat.config.shardingQuery.ShardingQueryRootConfig$LogicTableConfig
      #物理表关系
      queryPhysicalTable:
        #deafultDatabase,数据源名字,集群名字,三个中配置一个即可
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          #物理库名
          schemaName: db1
          #物理表名
          tableName: address
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db2
          tableName: address
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db3
          tableName: address
      #分片算法,可以不配置,配置后会使用分片字段和分片算法进行拉取数据,不配置将会扫描所有节点
      function: partitionByLong
      #分片字段
      columns:
        - ID
      #分片算法的参数,如果不配置将会读取分片算法配置中参数
      properties: &id001
        partitionCount: 2,1
        partitionLength: 256,512
      ranges: &id002 {}
    TRAVELRECORD: !!io.mycat.config.shardingQuery.ShardingQueryRootConfig$LogicTableConfig
      queryPhysicalTable:
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db1
          tableName: TRAVELRECORD
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db1
          tableName: TRAVELRECORD2
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db1
          tableName: TRAVELRECORD3
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db2
          tableName: TRAVELRECORD
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db2
          tableName: TRAVELRECORD2
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db2
          tableName: TRAVELRECORD3
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db3
          tableName: TRAVELRECORD
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db3
          tableName: TRAVELRECORD2
        - dataNodeName: null
          hostName: mytest3306a
          replicaName: null
          schemaName: db3
          tableName: TRAVELRECORD3
      columns:
        - ID
      function: partitionByLong
      properties: *id001
      ranges: *id002
type: null
version: 0
filePath: null

```

分布式查询使用要点

仅仅支持select 语句

sql要求写清楚逻辑库逻辑表,不能省略逻辑库

不同数据类型之间的转换可能与mysql的行为不一样

通过mycat的日志可以看到sql的执行计划以及谓词下推情况