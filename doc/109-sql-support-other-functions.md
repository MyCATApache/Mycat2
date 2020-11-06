## Mycat2-SQL支持手册-其他函数

### 其他函数



#### 支持

##### ANY_VALUE

###### Syntax

```sql
ANY_VALUE(column)
```

用于支持ONLY_FULL_GROUP_BY



##### UUID

###### Syntax

```sql
UUID()
```

使用java实现的UUID实现，返回类型是字符串



##### UUID_SHORT

###### Syntax

使用java实现的UUID实现，返回类型是long（64）



#### 不支持

##### GET_LOCK

###### Syntax

```
GET_LOCK(str,timeout)
```



##### NET6_ATON

###### Syntax

```
INET6_ATON(expr)
```



##### INET6_NTOA

###### Syntax

```
INET6_NTOA(expr)
```



##### INET_ATON

###### Syntax

```
INET_ATON(expr)
```



##### INET_NTOA

###### Syntax

```
INET_NTOA(expr)
```



##### IS_FREE_LOCK

###### Syntax

```
IS_FREE_LOCK(str)
```



##### IS_IPV4

###### Syntax

```
IS_IPV4(expr)
```



##### IS_IPV4_COMPAT

###### Syntax

```
IS_IPV4_COMPAT(expr)
```



##### IS_IPV6

###### Syntax

```
IS_IPV6(expr)
```



##### IS_USED_LOCK

###### Syntax

```
IS_USED_LOCK(str)
```



##### MASTER_GTID_WAIT

###### Syntax

```
MASTER_GTID_WAIT(gtid-list[, timeout)
```



##### MASTER_POS_WAIT

###### Syntax

```
MASTER_POS_WAIT(log_name,log_pos[,timeout,["connection_name"]])
```



##### NAME_CONST

###### Syntax

```
NAME_CONST(name,value)
```



##### RELEASE_ALL_LOCKS

###### Syntax

```
RELEASE_ALL_LOCK(str)
```



##### RELEASE_LOCK

###### Syntax

```
RELEASE_LOCK(str)
```



##### SLEEP

###### Syntax

```
SLEEP(duration)
```



##### VALUES / VALUE

###### Syntax

```
VALUE(col_name) 
```

```sql
VALUES(col_name) 
```

