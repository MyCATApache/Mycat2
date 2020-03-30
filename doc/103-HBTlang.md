

# HBTlang-Mycat2.0

author:chenjunwen 2020-3-16

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

[TOC]



## HBT(Human Brain Tech)简介

HBT在Mycat2中表现为关系表达式领域驱动语言(Relation DSL).

在设计上是Mycat2的运行时的中间语言,关于查询的HBT可以实现与SQL,其他查询优化器,查询编译器的关系表达式相互甚至与SQL DSL框架中的DSL转换.HBT也支持直接从文本和编写代码的方式构建.



## 使用HBT解决什么问题?

1.允许用户直接编写关系表达式实现功能,不同的SQL方言可以对应同一套关系表达式

2.运行用户运行自定义函数

3.免去优化过程,用户编写的关系表达式可以就是执行计划

4.允许使用函数宏展开关系表达式,比如给逻辑表函数宏指定分片范围自动扩展成对应的物理表

5.允许SQL与关系表达式一并编写,例如叶子是SQL,根是Mycat的运算节点

6.可移植到其他平台运行

7.使用DSL作为中间语言下发到其他Mycat节点上运行

8.方便验证测试



## 基本HBT表达式



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
table(fields(fieldType(id,`integer`)),values(1,2,3))
```



#### fieldType

##### 列类型

列名

列类型

可空

精度

小数位数

##### 三种构造方式

```java
fieldType(columnName,columnType)
fieldType(columnName,columnType,columnNullable)
fieldType(columnName,columnType,columnNullable,precision,scale)
    
即
fieldType(String columnName, String columnType)
\\columnNullable = true ,precision = null,scale = null

fieldType(String columnName, String columnType, boolean columnNullable)
\\precision = null,scale = null

fieldType(String columnName, String columnType, boolean columnNullable,Integer precision, Integer scale)
```



#### 字段类型(不区分大小写)

| 名称    | precision(精度) - scale(小数位数) | java类型 |
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



#### 语法辅助

###### fields

列类型列表

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

map表达式的简化,对数据源全列名进行更名,该表达式是一个补充的表达式



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

#### 集合操作

| 名称     | 类型 | 参数数量 | 参数               |
| -------- | ---- | -------- | ------------------ |
| 集合操作 | rel  | 至少2个  | 大于等于两个数据源 |



```
unionAll
并
unionDistinct
并,去重
exceptAll
除
exceptDistinct
除,去重
intersectAll
交
intersectDistinct
交,去重
```

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
//io.mycat.hbt.ast.HBTOp#INTERSECT_ALL
//io.mycat.hbt.ast.HBTOp#INTERSECT_DISTINCT
//io.mycat.hbt.ast.HBTBuiltinHelper#set(io.mycat.hbt.HBTOp, java.util.List<io.mycat.hbt.ast.base.Schema>)
set(HBTOp.UNION_ALL, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")))
```



#### distinct

| 名称 | 类型 | 参数数量 | 参数       |
| ---- | ---- | -------- | ---------- |
| 去重 | rel  | 一个     | 一个数据源 |



去重,该表达式是补充的表达式



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



##### avg

| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| avg | agg expr  | 1        | 列名标识符 |

扩展型聚合函数,会被优化器转换成sum()/count()

输入值要求是数字类型



##### count
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| count | agg expr  | 任意        | 多个列名标识符或无 |

当有多个列名标识符作为参数的时候,,返回其值不为null 的输入行数

无参的时候返回输入的行数



##### max
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| max | expr  | 1        | 列名标识符 |


输入值要求是数字类型
返回所有输入值中的最大值




##### min
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| min | agg expr  | 1        | 列名标识符 |



输入值要求是数字类型

返回所有输入值中的最小值




##### sum
| 名称     | 类型 | 参数数量 | 参数            |
| -------- | ---- | -------- | --------------- |
| sum | agg expr  | 1        | 列名标识符 |

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



### Join

Join操作

### innerJoin

inner join

内连接

在表中存在至少一个匹配时，返回行。



### leftJoin

left outer Join

左连接

从左表返回所有的行，即使在右表中没有匹配的行。



### rightJoin

right outer Join

右连接

从右表返回所有的行，即使在左表中没有匹配的行



### fullJoin

full outer Join

全连接

只要其中某个表存在匹配,就会返回行



### semiJoin

left semi join

半连接,左半连接

当条件成立时,返回左表的行



### antiJoin

anti semi join

反连接,反半连接

当条件不成立时,返回左表的行



文本

| 名称                                           | 类型 | 参数数量 | 参数                              |
| ---------------------------------------------- | ---- | -------- | --------------------------------- |
| innerJoin,leftJoin,rightJoin,semiJoin,antiJoin | rel  | 三个     | 条件:布尔表达式,左数据源,右数据源 |





```sql
//innerJoin
//leftJoin
//rightJoin
//semiJoin
//antiJoin
innerJoin(`id0` = `id`,fromTable(db1,travelrecord)
          .map(`id` as `id0`),fromTable(db1,travelrecord))
```



java

```java
new JoinSchema(HBTOp.INNER_JOIN,
                eq(id("id0"), id("id"))
                , map(fromTable("db1", "travelrecord"), Arrays.asList(as(new Identifier("id"), new Identifier("id0")))),
                fromTable("db1", "travelrecord")
```





### 列名冲突

两个数据源中字段名相同的情况下,使用as表达式建立别名后,才可以在条件里正确使用



### 行类型变更

join操作后,行类型是左数据源+右数据源的列,

比如上述的

`fromTable(db1,travelrecord).map(id as id0)`,`fromTable(db1,travelrecord)`

行类型分别是`(id0)`,`(id,user_id)`

innerJoin操作后变为`(id0,id,user_id)`



### OrderBy

对数据源排序

| 名称    | 类型 | 参数数量    | 参数             |
| ------- | ---- | ----------- | ---------------- |
| OrderBy | rel  | 1+order数量 | 数据源,多个order |



文本

```sql
fromTable(db1,travelrecord).orderBy(order(id,ASC), order(user_id,DESC))

orderBy(fromTable(db1,travelrecord),order(id,ASC), order(user_id,DESC))
```



java

```java
orderBy(fromTable("db1", "travelrecord"), Arrays.asList(order("id", Direction.ASC), order("user_id", Direction.DESC)))
```



#### 语法辅助

order(列名,asc或者desc,不区分大小写)



### limit

要数据源返回的行的一部分

| 名称  | 类型 | 参数数量        | 参数                |
| ----- | ---- | --------------- | ------------------- |
| limit | rel  | 1+多个order数量 | 数据源,offset,limit |



文本

```sql
fromTable(db1,travelrecord).limit(1,2)
limit(fromTable(db1,travelrecord),1,2)
```



java

```
limit(fromTable("db1", "travelrecord"), 1, 2)
```



## HBT-扩展表达式

### fromSql

sql作为数据源

| 名称    | 类型 | 参数数量 | 参数   |
| ------- | ---- | -------- | ------ |
| fromSql | rel  | 2或3     | 见下面 |

三个参数



1.列类型信息

2.查询目标名字

3.任意sql,返回的结果集的类型必须与给出的列类型一致

```sql
fromSql(fields(fieldType(`id`,`integer`),fieldType(`user_id`,`integer`),targetName,'select * from db1.travelrecord')
```



两个参数

2.查询目标名字

3.sql,自动推导列信息,暂时,sql只能配置在元数据配置的dataNode中的表名

```sql
fromSql(targetName,'select * from db1.travelrecord')
```



### filterFromTable

1.强调可下推的谓词

2.避免逻辑表与物理表耦合

| 名称            | 类型 | 参数数量 | 参数                                 |
| --------------- | ---- | -------- | ------------------------------------ |
| filterFromTable | rel  | 3        | 布尔表达式,逻辑库标识符,逻辑表标识符 |



等价

```sql
filterFromTable(`id` = 1,db1,travelrecord)//强调可下推
<=>
fromTable(`id` = 1,db1,travelrecord).filter(`id` = 1)//不强调下推
```



下推

```
filterFromTable(`id` = 1,db1,travelrecord)//经过上下文分析生成查询的目标和查询的SQL
=>
fromSql('targetName','select * from db1.travelrecord where `id` = 1')//可能的SQL
```



文本

```sql
filterFromTable(`id` = 1,db1,travelrecord)
filterFromTable(`id` = 1 or `id` = 2 ...,db1,travelrecord)
filterFromTable(1<= `id` and `id` <= 10000,db1,travelrecord)
```



  布尔表达式必须是简单的形式,不支持任何计算,别名



### fromRelToSql

转换HBT到SQL

极少数转换可能会失败,即使转换成功,SQL未必是数据库支持的,转换目标方言默认是mysql

| 名称         | 类型 | 参数数量 | 参数 |
| ------------ | ---- | -------- | ---- |
| fromRelToSql | rel  | 2        |      |



文本

```sql
fromRelToSql(targetName,fromTable('db1','travelrecord').filter(`id` = 1).map(`id`))
=>等价转换
fromSql(targetName,'select db1.travelrecord.id from db1.travelrecord where `id` = 1')
```



### modifyFromSql

向目标执行返回值不是结果集的SQL

| 名称          | 类型   | 参数数量 | 参数           |
| ------------- | ------ | -------- | -------------- |
| modifyFromSql | update | 2        | 目标标识符,sql |

执行结果

updateCount

lastInsertId



文本

```sql
modifyFromSql(targetName,'delete db1.travelrecord1')
```



### mergeModify

合拼多个modifyFromSql结果

即

updateCount 求和

lastInsertId 取mergeModify中最大值



| 名称        | 类型   | 参数数量                  | 参数          |
| ----------- | ------ | ------------------------- | ------------- |
| mergeModify | update | 一个或者多个modifyFromSql | modifyFromSql |



文本

```sql
mergeModify(
    modifyFromSql(targetName,'delete from db1.travelrecord1'),
    modifyFromSql(targetName2,'delete from db2.travelrecord1')
    )
```



## HBT-命令表达式

### explain

1.显示数据源的列类型,辅助编写hbt

2.显示执行计划



文本

```sql
explain filterFromTable(`id` = 1,db1,travelrecord)
```



## HBT-标量表达式(运算符)

计算结果是一个特定数据类型的标量值的表达式



| 运算符函数名 | 糖   | 描述 | 算符优先级 | 参数数量 |布尔表达式|
| ------------ | ---- | ------ | ---- | -------- | --------|
| eq           | =    |   	等于     | 10 | 2        | y |
| ne           | <>,!= |  	 不等于      | 10 | 2        | y |
| gt           | >  | 大于 | 11 | 2        | y |
| lt           | <   | 小于       | 11 | 2        | y|
| gte          | >=     | 大于等于 | 11 | 2        |y |
| lte          | <=    | 小于等于 | 11 | 2        | y|
| add          | +     | 算术加法 | 13 | 2        | n|
| minus        | -    |  算术减法      | 13 |  2        |n |
| and          | 无     |   逻辑与     | 6 |  2        |y |
| or          | 无     |   逻辑或     | 5 |  2        | y |
| not          | 无     | 逻辑非       |   无   | 1 |y |
| as          | 无     | 别名       | 3 | 2        | n |
| cast          | 无     |  类型转换      | 无 | 2        | n |
| ref | 无     |    关联连接数据源引用    | 无 | 2        | n|


上述描述的运算符都是左结合,其他情况使用函数调用

当运算符参数数量为2并且具有优先级的时候可以使用中缀表达式编写文本,解析器自动完成转换为函数调用并解糖



```java
1+2 => add(1,2)
```



需要用到其他函数,请提PR



支持中缀表达式编写文本的关系表达式,同理,也可以被解析器完成转换,

```sql
fromTable(db1,travelrecord) unionAll  fromTable(db1,travelrecord) 
=>
unionAll(fromTable(db1,travelrecord),fromTable(db1,travelrecord))
```



算符优先级都是1,左结合

```sql
unionAll,
unionDistinct,
exceptDistinct,
exceptAll,
rename,
groupBy
distinct,
orderBy
alias//仅用于聚合函数 ,
distinct//用于关系表达式,聚合函数 ,
approximate//仅用于聚合函数 ,
ignoreNulls//仅用于聚合函数 ,
filter//用于关系表达式,聚合函数 ,
```



### 类型转换与别名

```
fromTable(db1,travelrecord).map(cast(id,float) as a)
```



| 名称 | 类型 | 参数数量 | 参数                                    |
| ---- | ---- | -------- | --------------------------------------- |
| cast | expr | 2        | 表达式,类型标识符(不接受待求值的表达式) |



| 名称 | 类型 | 参数数量 | 参数                                |
| ---- | ---- | -------- | ----------------------------------- |
| as   | expr | 2        | 表达式,标识符(不接受待求值的表达式) |



### 内置常用函数

| 名称  | 类型 | 参数数量 | 参数               |
| ----- | ---- | -------- | ------------------ |
| lower | expr | 1        | 表达式(字符串类型) |

把参数转成小写

| 名称  | 类型 | 参数数量 | 参数               |
| ----- | ---- | -------- | ------------------ |
| upper | expr | 1        | 表达式(字符串类型) |

把参数转成大写

| 名称   | 类型 | 参数数量 | 参数   |
| ------ | ---- | -------- | ------ |
| isnull | expr | 1        | 表达式 |

表达式的结果是否null

| 名称   | 类型 | 参数数量 | 参数            |
| ------ | ---- | -------- | --------------- |
| nullif | expr | 2        | 表达式1,表达式2 |

如果两表达式的结果相同,则返回null

| 名称      | 类型 | 参数数量 | 参数   |
| --------- | ---- | -------- | ------ |
| isnotnull | expr | 1        | 表达式 |

表达式的结果是否非null





## HBT-列名引用

代数关系表达式中的表达式引用列名的方法以及存在的规则

1.在HBT里面,一般来说,一个数据源上的列名是唯一的,不能存在相同的列名,不能同时以$开头和以数字结尾

2.列表使用``包裹强调是标识符

```sql
fromTable(db1,travelrecord).filter(`id` = 1)
```

3.在涉及两个数据源的情况下,需要遇上列名相同的情况,as表达式或者rename关系表达式把其中一个数据源的列名改变,一般风格是添上0为结尾

```sql
innerJoin(`id0` = `id`,fromTable(db1,travelrecord)
          .map(`id` as `id0`),fromTable(db1,travelrecord))
```

4.支持下标引用列名

以$[0-9]格式,以下标引用第一个数据源的列名

```sql
fromTable(db1,travelrecord).filter(`$0` = 1)//引用位置0的列表,即id
```

在join关系表达式下,以$$[0-9]格式,以下标引用第二个数据源的列名

```sql
innerJoin(`$0` = `$$0`,fromTable(db1,travelrecord),fromTable(db1,travelrecord))
```

$0引用第一个数据源的id

$1引用第一个数据源的user_id

$$0引用第二个数据源的id

$$1引用第二个数据源的user_id



编译器会根据上下文判断列名是哪个数据源哪个下标,最终转换成下标表示,并解决列名冲突,在相同列名上添加0.





## HBT-词法

HBT词法分析就是MySQL的词法

编码为UTF8

不支持字符串里嵌入二进制数据,使用X'4D2AFF'这种格式描述二进制数据

这里不再重复描述MySQL词法



词法可以区分字面量和标识符,其中字面量一般就是值





## HBT-语法

### 基本形式

语法的设计选取了函数调用语法

```java
function(arg1,arg2)
```

作为基本形式,任何语法糖都会语法分析器编译成该形式



### 语法糖



#### 中缀表达式语法糖

在基本形式的基础上使用中缀表达式简化标量表达式的编写,尤其是and or表达式

```java
`id` = 1 and `user_id` = 2 => and(eq(`id`,1),eq(`user_id`,2))
```



#### 函数别名语法糖

为解决eq,lt等函数名并不接近sql常用的运算符= >的问题

语法分析器在判断它们是函数之后,会把它们的函数名指向最终的函数名



#### 链式调用语法糖

在基本形式的基础上使用链式调用语法减少深层括号嵌套的影响

```java
fromSql(aaaa).filter(bbbb) => filter(fromSql(aaaa),bbbb)
```



### 语法辅助(函数)

用于分隔语法节点层次



### 函数的参数

HBT语法上使用函数调用方式,但是实际上是使用文本转换成语法树表达builder模式

参数的实际含义取决于所在的函数名,参数位置

例如filter的第二个参数本质上是接收语法树而非表达式的语法树的求值结果



## HBT类型转换

HBT无隐式的类型转换,一般使用cast显式编写

对于整型数值的算术运算需要保持精度的情况,比如avg运算,请在avg的参数表达式上先转换成float等浮点类型



## 高级用法(少数情况出现,一般不使用)

### 额外类型

以下类型未正式支持

| 名称                 | precision(精度) - scale(小数点后的位数) | java类型 |
| -------------------- | --------------------------------------- | -------- |
| IntervalYear         | no-no                                   |          |
| IntervalYearMonth    | no-no                                   |          |
| IntervalMonth        | no-no                                   |          |
| IntervalDay          | no-no,yes-no                            |          |
| IntervalDayHour      | no-no,yes-no                            |          |
| IntervalDayMinute    | no-no,yes-no                            |          |
| IntervalDaySecond    | no-no,yes-no                            |          |
| IntervalHour         | no-no,yes-no                            |          |
| IntervalHourMinute   | no-no,yes-no                            |          |
| IntervalMinute       | no-no,yes-no                            |          |
| IntervalMinuteSecond | no-no,yes-no                            |          |
| IntervalSecond       | no-no,yes-no                            |          |



### 聚合函数



额外构建参数,需要规则甚至执行器配合,不推荐使用,不在HBT的目标之内


| 参数        | 描述                                                         |
| ----------- | ------------------------------------------------------------ |
| distinct    | 如果distinct存在, 则聚合前进行distinct运算过滤,默认不开启    |
| approximate | 不同函数不同,默认不开启,用于特殊函数                         |
| ignoreNulls | 如果ignoreNulls存在,则聚合前使用布尔表达式过滤,优化器会用自动添加Order表达式,默认不开启 |
| filter      | 如果filter存在,则聚合前使用布尔表达式过滤,优化器会用自动添加Order表达式,默认不开启 |
| orderKeys   | 如果orderKeys存在,则聚合前先排序,优化器会用自动添加filter表达式,默认不开启 |
| alias       | 为聚合函数对应的列设置别名,默认不开启                        |



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



### 关联连接

对应关联子查询

循环获取左表的行,使用该行的值作为查询用到的参数重新获取右表数据源,之后进行内连接或者左连接

具体执行一般实现为NestedLoops

### correlateInnerJoin

关联内连接



### correlateLeftJoin

关联左连接



构建步骤

1.组合两个数据源而成为新的数据源,并为一个数据源建立别名供第二个数据源引用

```sql
correlateInnerJoin(别名,数据源1 ,数据源2)
correlateLeftJoin(别名,数据源1 ,数据源2)
```

2.第二个数据源引用别名

```sql
ref(别名,字段)
```

实际上别名就是数据源1

3.综上

text

```sql
correlateInnerJoin(`t`,
                   table(fields(fieldType(id0,integer,false)),values(1,2,3,4)) , fromTable(`db1`,`travelrecord`).filter(ref(`t`,`id0`) = `id`)))
```



java

```java
  Schema db0 = table(Arrays.asList(fieldType("id0", "Integer",false)), Arrays.asList(1, 2, 3, 4));
        Schema db1 = filter(fromTable("db1", "travelrecord"), eq(ref("t", "id0"), new Identifier("id")));
        Schema schema = correlate(CORRELATE_INNER_JOIN, "t", db0, db1);
```



## 文档更改记录

2020.3.16-2020-3-17 完成文档编写 (chenjunwen)

