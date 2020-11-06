

# mycat 2.0-readme

author:junwen  2020-3-15

联系: qq:  294712221

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

开发日志: <https://github.com/MyCATApache/Mycat2/blob/master/doc/101-todo-history-list.md>
项目地址: <https://github.com/MyCATApache/Mycat2>

## 特点

1.proxy透传报文,使用buffer大小与结果集无关

2.proxy透传事务,支持XA事务,jdbc本地事务

3.支持分布式查询

## 限制

暂不支持预处理(客户端可以开启客户端预处理解决这个问题),游标等



测试版本的mycat2无需账户密码即可登录



## Mycat2流程

客户端发送SQL到mycat2,mycat2拦截对应的SQL执行不同的命令,对于不需要拦截处理的SQL,透传到有逻辑表的mysql,这样,mycat2对外就伪装成mysql数据库



### 拦截器

#### SQL匹配

```yaml
interceptor: #拦截器,如果拦截不了,尝试use schema,试用explain可以看到执行计划,查看路由
  defaultHanlder: {command: execute , tags: {targets: defaultDs,forceProxy: true}}
  schemas: [{
              tables:[ 'db1.travelrecord','db1.address1'],#sql中包含一个表名,就进入该匹配流程
              sqls: [
              {sql: 'select {selectItems} from {any}',command: distributedQuery },
              {sql: 'delete {any}',command: execute,....}
              ],
            },
            {
              tables:[ 'db1.company'],
              sqls: [
              {sql: 'select {selectItems} from {any}',command: execute ,....}
              ],
            },
  ]
  sqls: [
  {name: useStatement; ,sql: 'use {schema};',command: useStatement}
  ]
  transactionType: xa #xa.proxy
```



#### 参数提取

sql中{name}是通配符,基于mysql的词法单元上匹配

同时把name保存在上下文中,作为命令的参数,所以命令的参数是可以从SQL中获得

```yaml
tags: {targets: defaultDs,forceProxy: true}
```

tags是配置文件中定下的命令参数

sql中的参数的优先级比tags高



#### SQL再生成

```yaml
  {name: 'mysql set names utf8', sql: 'SET NAMES {utf8}',explian: 'SET NAMES utf8mb4'  command: execute , tags: {targets: defaultDs,forceProxy: true}}
```

SQL被'SET NAMES utf8mb4'替换



```yaml
  {name: 'select n', sql: 'select {n}',explain: 'select {n}+1' command: execute , tags: {targets: defaultDs,forceProxy: true}},
```



拦截器会对use {schema}语句处理,得出不带schema的sql的table是属于哪一个schema.当mycat2发生错误的时候,会关闭连接,此时保存的schema失效,重新连接的时候请重新执行use schema语句





## 命令

##### explain

查看执行计划

```yaml
{name: explain,sql: 'EXPLAIN {statement}' ,command: explain}, #explain一定要写库名
```



##### commit

```yaml
{name: commit,sql: 'commit',command: commit},{name: commit;,sql: 'commit;',command: commit},
```



##### begin

```yaml
{name: begin; ,sql: 'begin',command: begin},{name: begin ,sql: 'begin;',command: begin},
```



##### rollback

```yaml
{name: rollback ,sql: 'rollback',command: rollback},{name: rollback;,sql: 'rollback;',command: rollback},
```



##### 切换数据库

```yaml
{name: useStatement ,sql: 'use {schema}',command: useStatement},
```



##### 开启XA事务

```yaml
{name: setXA ,sql: 'set xa = on',command: onXA},
```



##### 关闭XA事务

```yaml
{name: setProxy ,sql: 'set xa = off',command: offXA},
```



##### 关闭自动提交

关闭自动提交后,在下一次sql将会自动开启事务,并不会释放后端连接

```yaml
{name: setAutoCommitOff ,sql: 'set autocommit=off',command: setAutoCommitOff},
```



##### 开启自动提交

```yaml
{name: setAutoCommitOn ,sql: 'set autocommit=on',command: setAutoCommitOn},
```



##### 设置事务隔离级别

```
READ UNCOMMITTED,READ COMMITTED,REPEATABLE READ,SERIALIZABLE
```

```yaml
{name: setTransactionIsolation ,sql: 'SET SESSION TRANSACTION ISOLATION LEVEL {transactionIsolation}',command: setTransactionIsolation},
```



##### execute执行SQL

forceProxy:true|false

强制SQL以proxy上运行,忽略当前事务



needTransaction:true|false

根据上下文(关闭自动提交)自动开启事务



getMetaData:true|false

true的时候不需要配置targets,自动根据sql路由

false的时候要配置targets



targets

sql发送的目标:集群或者数据源的名字



balance

当targets是集群名字的时候生效,使用该负载均衡策略



executeType

QUERY执行查询语句,在proxy透传下支持多语句,否则不支持

QUERY_MASTER执行查询语句,当目标是集群的时候路由到主节点

INSERT执行插入语句

UPDATE执行其他的更新语句,例如delete,update,set



## MetaData表



#### 分片类型

##### 自然分片

单列或者多列的值映射单值,分片算法使用该值计算数据节点范围

##### 动态分片

单列或者多列的值在分片算法计算下映射分片目标,目标库,目标表,得到有效的数据节点范围



例子1

id ->集群名字

id->库名

id->表名



例子2

任意值->固定集群名字

address_name->库名

user_name->表名



例子3

任意值->固定集群名字,固定库名

address_name->表名

user_name->表名



总之能通过条件中存在的字段名得出存储节点三元组即可

多列值映射到一个值暂时不支持,后续支持



## MetaData支持的SQL

##### 查询SQL

```yaml
query:
      values
  |   WITH withItem [ , withItem ]* query
  |   {
          select
      |   selectWithoutFrom
      |   query UNION [ ALL | DISTINCT ] query
      |   query EXCEPT [ ALL | DISTINCT ] query
      |   query MINUS [ ALL | DISTINCT ] query
      |   query INTERSECT [ ALL | DISTINCT ] query
      }
      [ ORDER BY orderItem [, orderItem ]* ]
      [ LIMIT [ start, ] { count | ALL } ]
      [ OFFSET start { ROW | ROWS } ]
      [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]

withItem:
      name
      [ '(' column [, column ]* ')' ]
      AS '(' query ')'

orderItem:
      expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]

select:
      SELECT [ STREAM ] [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]

selectWithoutFrom:
      SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ ( LEFT | RIGHT | FULL ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

joinCondition:
      ON booleanExpression
  |   USING '(' column [, column ]* ')'

tableReference:
      tablePrimary
      [ FOR SYSTEM_TIME AS OF expression ]
      [ matchRecognize ]
      [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'
  |   [ LATERAL ] '(' query ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

columnDecl:
      column type [ NOT NULL ]

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'
  |   GROUPING SETS '(' groupItem [, groupItem ]* ')'

```



以下命令请使用之前使用explian查看执行计划,检查sql拆分是否正确

##### 插入SQL

```yaml
{sql: 'insert {any}',command: execute, tags: {executeType: INSERT,getMetaData: true,needTransaction: true }},
```



##### 更新SQL

```yaml
{sql: 'update {any}',command: execute,tags: {executeType: UPDATE,getMetaData: true ,needTransaction: true }},
```



##### 删除SQL

```yaml
{sql: 'delete {any}',command: execute,tags: {executeType: UPDATE,getMetaData: true,needTransaction: true  }}
```





## 分片算法配置

https://github.com/MyCATApache/Mycat2/blob/master/doc/17-partitioning-algorithm.md