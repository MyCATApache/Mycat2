## Mycat2-SQL支持手册-加密和哈希和压缩功能

### 加密，哈希和压缩功能



#### 支持

##### MD5

###### Syntax

```sql
MD5(str)
```

调用java的MD5函数进行运算

如果参数值为null,就返回null



#### 临时支持

使用jdbc执行



##### UNCOMPRESSED_LENGTH

###### Syntax

```sql
UNCOMPRESSED_LENGTH(compressed_string)
```



#### 不支持

##### **AES_DECRYPT**

###### Syntax

```sql
AES_DECRYPT(crypt_str,key_str)
```



##### AES_ENCRYPT

###### Syntax

```sql
AES_ENCRYPT(str,key_str)
```



##### COMPRESS

###### Syntax

```sql
COMPRESS(str)
```



##### DECODE

###### Syntax

```sql
DECODE(crypt_str,pass_str)COMPRESS(string_to_compress)
```



##### DES_DECRYPT

###### Syntax

```sql
DES_DECRYPT(crypt_str[,key_str])
```



##### DES_ENCRYPT

###### Syntax

```sql
DES_ENCRYPT(str[,{key_num|key_str}])
```



##### DECODE_HISTOGRAM

###### Syntax

```sql
DECODE_HISTOGRAM(hist_type,histogram)
```



##### ENCODE

###### Syntax

```sql
ENCODE(str,pass_str)
```



##### ENCRYPT

###### Syntax

```sql
ENCRYPT(str[,salt])
```



##### OLD_PASSWORD

###### Syntax

```sql
OLD_PASSWORD(str)
```



##### PASSWORD

###### Syntax

```sql
PASSWORD(str)
```



##### SHA1

###### Syntax

```sql
SHA1(str), SHA(str)
```



##### SHA2

###### Syntax

```sql
SHA2(str,hash_len)
```



##### UNCOMPRESS

###### Syntax

```sql
UNCOMPRESS(string_to_uncompress)
```

