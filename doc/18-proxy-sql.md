# mycat 2.0 proxy sql

author:junwen 2019-6-14

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

```sql
use {schema}
```

该sql切换mycat session当前的schema

```sql
set autocommit = 1;
set autocommit = 0;
set autocommit on;
set autocommit off;
```

该sql切换mycat session当前的autocommit状态

```sql
set names {charset}
```

该sql切换mycat session当前的charset变量

```sql
SET character_set_results
```

该sql切换mycat session当前的character_set_results变量

```sql
SET [GLOBAL | SESSION] TRANSACTION
    transaction_characteristic [, transaction_characteristic] ...

transaction_characteristic: {
    ISOLATION LEVEL level
  | access_mode
}

level: {
     REPEATABLE READ
   | READ COMMITTED
   | READ UNCOMMITTED
   | SERIALIZABLE
}

access_mode: {
     READ WRITE
   | READ ONLY
}
```

GLOBAL级别 返回错误

带有access_mode会被记录,在sql

多于一个transaction_characteristic,返回错误,即带有,

```sql
show databases
```

返回mycat所有逻辑库名字

show tables

返回mycat当前schema的所有逻辑表名字

describe