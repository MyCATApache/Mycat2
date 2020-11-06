## Mycat2-SQL支持手册-流程控制函数

# 流程控制函数



#### 支持

##### CASE OPERATOR

###### Syntax

```sql
CASE value WHEN [compare_value] THEN result [WHEN [compare_value] THEN
result ...] [ELSE result] END

CASE WHEN [condition] THEN result [WHEN [condition] THEN result ...]
[ELSE result] END
```



##### IFNULL

###### Syntax

```sql
IFNULL(expr1,expr2)
```



##### NULLIF

###### Syntax

```sql
NULLIF(expr1,expr2)
```



##### 不支持

##### IF Function

###### Syntax

```sql
IF(expr1,expr2,expr3)
```

