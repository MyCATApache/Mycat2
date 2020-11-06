## Mycat2-SQL支持手册-信息函数

### 信息函数



#### 支持

##### CONNECTION_ID

###### Syntax

```sql
CONNECTION_ID()
```

Mycat的实现与Mysql不同，该ID仅仅标记会话，而与线程无关

结果是long(64位)



##### CURRENT_USER

##### SESSION_USER

##### USER

###### Syntax

```sql
CURRENT_USER, CURRENT_USER()，SESSION_USER，SESSION_USER()，USER,USER()
```

返回连接的用户，与Mysql不同，上述的函数是等价的。

结果格式

```sql
user@host
```



##### DATABASE

##### SCHEMA

###### Syntax

```sql
DATABASE()
```

返回当前默认数据库的名称。



##### VERSION

###### Syntax

```sql
VERSION()
```

返回模拟的Mysql版本号

默认是8.0.19



##### LAST_INSERT_ID

###### Syntax

```sql
LAST_INSERT_ID(), LAST_INSERT_ID(expr)
```

LAST_INSERT_ID（）（无参数）返回由于最近执行的INSERT语句而成功为[AUTO_INCREMENT](https://mariadb.com/kb/en/auto_increment/)列成功插入的第一个自动生成的值 。如果没有成功插入任何行，则LAST_INSERT_ID（）的值将保持不变。

如果给LAST_INSERT_ID（）提供了一个参数，则它将返回表达式的值，而对LAST_INSERT_ID（）的下一次调用将返回相同的值。



#### 不支持

##### BENCHMARK

###### Syntax

```sql
BENCHMARK(count,expr)
```



##### BINLOG_GTID_POS

###### Syntax

```sql
BINLOG_GTID_POS(binlog_filename,binlog_offset)
```



##### CHARSET

###### Syntax

```sql
CHARSET(str)
```



##### COERCIBILITY

###### Syntax

```sql
COERCIBILITY(str)
```



##### COLLATION

###### Syntax

```sql
COLLATION(str)
```



##### CURRENT_ROLE

###### Syntax

```sql
CURRENT_ROLE, CURRENT_ROLE()
```



##### DECODE_HISTOGRAM

###### Syntax

```sql
DECODE_HISTOGRAM(hist_type,histogram)
```



##### DEFAULT

##### Syntax

```sql
DEFAULT(col_name)
```



##### FOUND_ROWS

##### Syntax

```sql
FOUND_ROWS()
```



##### LAST_VALUE

##### Syntax

```sql
LAST_VALUE(expr,[expr,...])
LAST_VALUE(expr) OVER (
  [ PARTITION BY partition_expression ]
  [ ORDER BY order_list ]
) 
```



##### PROCEDURE ANALYSE

##### Syntax

```sql
analyse([max_elements[,max_memory]])
```



##### ROW_COUNT

##### Syntax

```sql
ROW_COUNT()
```

