

# HBTlang-Mycat2.0

author:chenjunwen 2020-3-16

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.



## HBT(Human Brain Tech)

HBT在Mycat2中表现为关系表达式领域驱动语言(Relation DSL).

在设计上是Mycat2的运行时的中间语言,关于查询的HBT可以实现与SQL,其他查询优化器,查询编译器的关系表达式相互甚至与SQL DSL框架中的DSL转换.HBT也支持直接从文本和编写代码的方式构建.



## 使用HBT解决什么问题?

1.允许用户直接编写关系表达式实现功能,不同的SQL方言可以对应同一套关系表达式

2.运行用户运行自定义函数

3.免去优化过程,用户编写的关系表达式可以就是最终的执行计划

4.允许使用函数宏展开关系表达式,比如给逻辑表函数宏指定分片范围自动扩展成对应的物理表

5.允许SQL与关系表达式一并编写,例如叶子是SQL,根是Mycat的运算节点

6.可移植到其他平台运行

7.使用DSL作为中间语言下发到其他Mycat节点上运行

8.方便验证测试



## HBT表达式



### fromTable

获得逻辑表的数据源

| name      | 类型 | 参数数量 | 参数                          |
| --------- | ---- | -------- | ----------------------------- |
| fromTable | rel  | 2        | 逻辑库的标识符,逻辑表的标识符 |



text

```sql
fromTable(db1,travelrecord)
```



java

```java
//io.mycat.hbt.ast.HBTBuiltinHelper#fromTable(java.lang.String, java.lang.String)
fromTable("db1", "travelrecord")
```



### table

匿名表,一种字面量构成的数据源

| 名称  | 类型 | 参数数量 | 参数                              |
| ----- | ---- | -------- | --------------------------------- |
| table | rel  | 至少两个 | 字段信息列表,字面量列表(一维列表) |





text

```sql
table(fields(fieldType(`1`,`integer`),fieldType(`2`,`varchar`)),values())
table(fields(fieldType(id,int)),values(1,2,3))
```



#### fieldType

字段信息,三种构造方式

```java
fieldType(String columnName, String columnType)
\\columnNullable = true ,precision = null,scale = null

fieldType(String columnName, String columnType, boolean columnNullable)
\\precision = null,scale = null

fieldType(String columnName, String columnType, boolean columnNullable,Integer precision, Integer scale)
```



#### 字段类型(不区分大小写)

| 名称    | precision(精度) - scale(小数点后的位数) | java类型 |
| ------- | ----------------- |--|
| Bool  |       no-no             |boolean|
| Tinyint |         no-no          |byte|
| Smallint   |       no-no            |short|
| Integer      |     no-no              |int|
| Bigint        |         no-no          |long|
|  Decimal       |         no-no,yes-no          |BigDecimal|
|  Float       |          no-no          |float|
|   Real      |          no-no         |float|
|   Double      |         no-no          |double|
|   Date      |          no-no         |Date ‘xxxx-xx-xx’|
|   Time      |         no-no,yes-no       |Time ‘xx:xx:xx’|
|   TimeWithLocalTimeZone|no-no,yes-no |Timestamp ‘xxxx-xx-xxxx:xx:xx’|
|   Timestamp    | no-no,yes-no           |Timestamp ‘xxxx-xx-xxxx:xx:xx’|
|   TimestampWithLocalTimeZone      |  no-no,yes-no                 |Timestamp ‘xxxx-xx-xxxx:xx:xx’|
|   Char |          no-no     |String|
|   Varchar |      no-no            |String|
|   Binary |               no-no   |byte[]|
|   Varbinary |          no-no        |byte[]|

以下类型未正式支持
| 名称    | precision(精度) - scale(小数点后的位数) | java类型 |
| ------- | ----------------- |--|
|   IntervalYear      |       no-no            ||
|   IntervalYearMonth      |    no-no               ||
|   IntervalMonth      |       no-no            ||
|   IntervalDay      |       no-no,yes-no             ||
|   IntervalDayHour      |        no-no,yes-no           ||
|   IntervalDayMinute      |      no-no,yes-no             ||
|   IntervalDaySecond      |       no-no,yes-no            ||
|   IntervalHour    |           no-no,yes-no        ||
|   IntervalHourMinute    |       no-no,yes-no            ||
|   IntervalMinute   |          no-no,yes-no         ||
|   IntervalMinuteSecond  |        no-no,yes-no           ||
|   IntervalSecond |             no-no,yes-no      ||



#### 语法辅助

###### fields

多个fieldType组成

###### values

多个字面值组成

解析器会根据fieldType的数量自动把数列转成二维表



java

```java
//io.mycat.hbt.ast.HBTBuiltinHelper#table(java.util.List<FieldType>, java.util.List<Object>)


table(Arrays.asList(fieldType("1", "integer"), fieldType("2", "varchar")), Arrays.asList())
//()
```



```java
table(Arrays.asList(fieldType("id", "integer")), Arrays.asList(1,2,3))
//(1),(2),(3)
```



```java
table(Arrays.asList(fieldType("id", "integer"),fieldType("id2", "integer")), Arrays.asList(1,2,3,4))
//(1,2),(3,4)
```



### map

| 名称 | 类型 | 参数数量 | 参数                  |
| ---- | ---- | -------- | --------------------- |
| map  | rel  | 至少两个 | 数据源,至少一个表达式 |

投影



text

```sql
table(fields(fieldType(id,integer),fieldType(id2,integer)),values(1,2))
.map(id2 as id4)//sugar

map(table(fields(fieldType(id,integer),fieldType(id2,integer)),values(1,2)),id2 as id4)

```



text

```sql
table(fields(fieldType(id,integer),fieldType(id2,integer))
.map(id + id2,id)//sugar
      
map(table(fields(fieldType(id,integer),fieldType(id2,integer)),values(1,2)),id + id2,id)
```



java

```java
//io.mycat.hbt.ast.HBTBuiltinHelper#map(Schema,List<Expr>)

map(table(Arrays.asList(fieldType("id", "integer"), fieldType("id2", "integer")), Arrays.asList()),Arrays.asList(as(new Identifier("id2"), new Identifier("id4"))))

map(table(Arrays.asList(fieldType("id", "integer"), fieldType("id2", "integer")), Arrays.asList()),
    Arrays.asList(add(new Identifier("id2"), new Identifier("id4")),newIdentifier("id")))
```



### rename

| 名称   | 类型 | 参数数量             | 参数                                  |
| ------ | ---- | -------------------- | ------------------------------------- |
| rename | rel  | **1+数据源的列数量** | 数据源,多个字段标识符(数据源的列数量) |

map表达式的简化,对数据源全列名进行更名,该表达式属于扩展表达式



text

```sql
table(fields(fieldType(`1`,`integer`,true),fieldType(`2`,`integer`,true)),values(1,2)).rename(`2`,`1`)

```



java

```java
//io.mycat.hbt.ast.HBTBaseTest#rename(Schema, List<String>)

rename(table(Arrays.asList(fieldType("1", "integer",true), fieldType("2", "integer",true)), Arrays.asList(1,2)), Arrays.asList("2", "1"))
      
```



### filter

选择,根据布尔表达式对行进行过滤

| 名称   | 类型 | 参数数量 | 参数                  |
| ------ | ---- | -------- | --------------------- |
| filter | rel  | 2        | 数据源,一个布尔表达式 |



text

```sql
fromTable(db1,travelrecord).filter(`id` = 1)

filter(fromTable(db1,travelrecord),`id` = 1)
```



java

```java
//io.mycat.hbt.ast.HBTBuiltinHelper#filter(Schema,Expr)
filter(fromTable("db1", "travelrecord"), eq(new Identifier("id"), new Literal(1)))
```



### Set

集合操作

| 名称     | 类型 | 参数数量 | 参数               |
| -------- | ---- | -------- | ------------------ |
| 集合操作 | rel  | 至少2个  | 大于等于两个数据源 |



##### unionAll

并



##### unionDistinct

并,去重



##### exceptAll

除



##### exceptDistinct

除,结果集去重



##### minusAll

减



##### minusDistinct

减,去重



##### intersectAll

交



##### intersectDistinct

交,去重



text

```sql
fromTable(db1,travelrecord) unionAll  fromTable(db1,travelrecord)

unionAll(fromTable(db1,travelrecord),fromTable(db1,travelrecord))
```



java

```java
//io.mycat.hbt.ast.HBTOp
//io.mycat.hbt.ast.HBTOp#UNION_ALL
//io.mycat.hbt.ast.HBTOp#UNION_DISTINCT
//io.mycat.hbt.ast.HBTOp#EXCEPT_ALL
//io.mycat.hbt.ast.HBTOp#EXCEPT_DISTINCT
//io.mycat.hbt.ast.HBTOp#MINUS_ALL
//io.mycat.hbt.ast.HBTOp#MINUS_DISTINCT
//io.mycat.hbt.ast.HBTOp#INTERSECT_ALL
//io.mycat.hbt.ast.HBTOp#INTERSECT_DISTINCT
//io.mycat.hbt.ast.HBTBuiltinHelper#set(io.mycat.hbt.HBTOp, java.util.List<io.mycat.hbt.ast.base.Schema>)
set(HBTOp.UNION_ALL, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")))
```



##### distinct

| 名称 | 类型 | 参数数量 | 参数       |
| ---- | ---- | -------- | ---------- |
| 去重 | rel  | 一个     | 一个数据源 |



去重,该表达式属于扩展表达式



text

```sql
fromTable(db1,travelrecord).distinct()

distinct(fromTable(db1,travelrecord))
```



java

```java
io.mycat.hbt.ast.HBTBuiltinHelper#distinct(Schema)
distinct(fromTable("db1", "travelrecord"))
```



### GroupBy

分组

| 名称 | 类型 | 参数数量 | 参数            |
| ---- | ---- | -------- | --------------- |
| 分组 | rel  | 2        | 分组键,聚合函数 |



text

```sql
fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`)),aggregating(avg(`id`))))

groupBy(fromTable(db1,travelrecord),keys(groupKey(`id`)),aggregating(avg(`id`)))
```



#### 分组键

```sql
groupKey(`id`)
groupKey(`id`,`id2`)
```

由一个或者多个以数据源列名组成



#### 聚合函数

基本构造参数

| 参数     | 描述                                                 |
| ----------- | ---------------------------------------------------- |
| 函数名      | 必须填写                                             |
| 参数列表    | 不同函数不同                                           |



额外构建参数,需要规则甚至执行器配合,不推荐使用,不在HBT的目标之内


| 参数      | 描述                                                 |
| ----------- | ---------------------------------------------------- |
| distinct    |如果distinct存在, 则聚合前进行distinct运算过滤,默认不开启                                               |
| approximate |   不同函数不同,默认不开启,用于特殊函数                                             |
| ignoreNulls |    如果ignoreNulls存在,则聚合前使用布尔表达式过滤,优化器会用自动添加Order表达式,默认不开启                                             |
| filter      | 如果filter存在,则聚合前使用布尔表达式过滤,优化器会用自动添加Order表达式,默认不开启 |
| orderKeys| 如果orderKeys存在,则聚合前先排序,优化器会用自动添加filter表达式,默认不开启 |
| alias| 为聚合函数对应的列设置别名,默认不开启 |



完整构建语法样例

text

```SQL
fromTable(db1,travelrecord)
.groupBy(keys(groupKey(`columnName`)),aggregating(avg(`columnName`).alias(a).distinct().approximate().ignoreNulls().filter(true).orderBy(order(user_id,DESC))))
```



java

```java
groupBy(fromTable("db1", "travelrecord"), Arrays.asList(groupKey(Arrays.asList(id(("columnName"))))),
                Arrays.asList(new AggregateCall("avg", Arrays.asList(id(("columnName")))).alias("a").distinct().approximate().ignoreNulls().filter(literal(true))
                        .orderBy(Arrays.asList(order("user_id", Direction.DESC)))))
```



##### avg

| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| avg | rel  | 1        | 列名标识符 |

扩展型聚合函数,会被优化器转换成sum()/count()
输入值要求是数字类型



##### count
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| count | rel  | 任意        | 多个列名标识符或无 |

当有多个列名标识符作为参数的时候,,返回其值不为null 的输入行数
无参的时候返回输入的行数



##### max
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| min | rel  | 1        | 列名标识符 |
输入值要求是数字类型
返回所有输入值中的最大值


##### min
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| min | rel  | 1        | 列名标识符 |

输入值要求是数字类型
返回所有输入值中的最小值


##### sum
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| sim | rel  | 1        | 列名标识符 |


输入值要求是数字类型
返回所有输入值的数值总和





#### 语法辅助

##### keys

由一个或者多个分组键组成

一般只有一个groupKey

多个groupKey组成GROUPING SETS

```java
keys(groupKey(`id`),groupKey(`id2`))
```



#####  aggregating

由一个或者多个聚合函数组成



java

```java
groupBy(fromTable("db1", "travelrecord"), Arrays.asList(groupKey(Arrays.asList(id(("id"))))),
                Arrays.asList(new AggregateCall("avg", Arrays.asList(id(("id")))).alias("a").distinct().approximate().ignoreNulls().filter(literal(true)).orderBy(Arrays.asList(order("user_id", Direction.DESC)))))
```



### innerJoin

在表中存在至少一个匹配时，INNER JOIN 关键字返回行。



### leftJoin

LEFT JOIN 关键字会从左表 (table_name1) 那里返回所有的行，即使在右表 (table_name2) 中没有匹配的行。



### rightJoin



### fullJoin



### semiJoin



### antiJoin



### correlateInnerJoin



### correlateLeftJoin

