## Mycat2-SQL支持手册-字符串函数

### 字符串函数



#### 字符串函数支持

#### Mycat内置

##### ASCII

###### Syntax

```
ASCII(str)
```



##### BIN

###### Syntax

```
BIN(N)
```



##### CHARACTER_LENGTH

###### Syntax

```
CHARACTER_LENGTH(str)
```



##### char_length

###### Syntax

```
CHARACTER_LENGTH(str)
```



##### CHR

###### Syntax

```
CHR(N)
```



##### CONCAT

###### Syntax

```
CONCAT(str1,str2) //2个参数
```



##### lcase

###### Syntax

```
LCASE(str)
```



##### lower

###### Syntax

```
LOWER(str)
```



##### left

###### Syntax

```
LEFT(str,len)
```



##### length

###### Syntax

```
LENGTH(str)
```



##### LENGTHB

###### Syntax

```
LENGTHB(str)
```



##### LIKE

###### Syntax

```
expr LIKE pat [ESCAPE 'escape_char']
expr NOT LIKE pat [ESCAPE 'escape_char']
```



##### ltrim

###### Syntax

```
LTRIM(str)
```



##### mid

###### Syntax

```
MID(str,pos,len)
```



##### NOT LIKE

###### Syntax

```
MID(str,pos,len)
```





#### Mycat内置临时支持



##### CONCAT

###### Syntax

```
CONCAT(str1,str2,...)//多个参数
```





#### Mycat使用JDBC实现

##### CONCAT_WS

###### Syntax

```
CONCAT_WS(separator,str1,str2,...)
```



##### bit_length

###### Syntax

```
BIT_LENGTH(str)
```



##### char

###### Syntax

```sql
CHAR(N,...)

//不支持CHAR(N,... [USING charset_name]) ，Mycat运算的字符串是JAVA内部编码
```



##### elt

###### Syntax

```sql
ELT(N, str1[, str2, str3,...])
```



##### export_set

###### Syntax

```
EXPORT_SET(bits, on, off[, separator[, number_of_bits]])
```



##### field

###### Syntax

```
FIELD(pattern, str1[,str2,...])
```



##### find_in_set

###### Syntax

```
FIND_IN_SET(pattern, strlist)
```



##### format

###### Syntax

```
FORMAT(num, decimal_position[, locale])
```



##### from_base64

###### Syntax

```
FROM_BASE64(str)
```



##### hex

###### Syntax

```
HEX(N_or_S)
```



##### insert

###### Syntax

```
INSERT(str,pos,len,newstr)
```



##### INSTR

###### Syntax

```
INSTR(str,substr)
```







##### locate

###### Syntax

```
LOCATE(substr,str), LOCATE(substr,str,pos)
```



##### lpad

###### Syntax

```
LPAD(str, len [,padstr])
```



##### make_set

###### Syntax

```
MAKE_SET(bits,str1,str2,...)
```





##### oct

外置

##### octet_length

外置

##### ord

外置

##### position

外置

##### quote

外置

##### repeat

外置

##### replace

外置

##### reverse

外置

##### right

外置

##### rpad

外置

##### rtrim

外置

##### space

外置

##### substr

外置

##### substring

外置

##### substring_index

外置

##### to_base64

外置

##### ucase

内置

##### upper

内置

##### unhex

外置







实验性支持

```
TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str)， TRIM([remstr FROM] str)
```



###### 不支持

##### BINARY 

一元运算符

##### Syntax

```
BINARY
```

例子

```
SELECT BINARY 'a' = 'A';
+------------------+
| BINARY 'a' = 'A' |
+------------------+
|                0 |
+------------------+
```



##### load_file

##### soundex

##### weight_string

##### EXTRACTVALUE

##### MATCH AGAINST

###### Syntax

```
MATCH (col1,col2,...) AGAINST (expr [search_modifier])
```



##### NOT REGEXP

###### Syntax

```
expr NOT REGEXP pat, expr NOT RLIKE pat
```

Regexp不支持