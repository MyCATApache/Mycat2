# 动态注解

author:junwen 2019-10-2

```yaml
lib:
  - io.mycat.lib.ProxyExport
  - io.mycat.lib.FinalCacheExport
  - io.mycat.lib.SQLModifierExport
  - io.mycat.lib.JdbcExport
  - io.mycat.lib.SessionMapExport
  - io.mycat.lib.CalciteExport
  - io.mycat.lib.CacheExport
  - io.mycat.lib.TransforFileExport
  - io.mycat.lib.ShardingQueryExport
schemaName: TESTDB1.TRAVELRECORD,TESTDB1.ADDRESS,TESTDB2.*
sql:
  - sql: use {schema}
    code: useSchemaThenResponseOk(matcher.group("schema"))

  - sql: show databases
    code: responseFinalCache("/sql/show_databases.sql")

  - sql:  show full tables from `TESTDB1` where table_type = 'BASE TABLE'
    code: responseFinalCache("/sql/show_full_tables_from_testdb1.sql")

  - sql:  show full tables from `TESTDB2` where table_type = 'BASE TABLE'
    code: responseFinalCache("/sql/show_full_tables_from_testdb2.sql")

  - sql:  describe `TESTDB1`.`travelrecord`
    code: responseFinalCache("/sql/describe_testdb1_travelrecord.sql")

  - sql:  describe `TESTDB1`.`address`
    code: responseFinalCache("/sql/describe_testdb1_address.sql")

  - sql: /*!40101 set @@session.wait_timeout=28800 */
    code: responseOk()

  - sql: commit {n}
    code: commitOnJdbc()

  - sql: begin
    code: beginOnJdbc()

  - sql: select {n}
    code: responseQueryCalcite(matcher.getSQLAsString())
  - sql: select 1
    code: transferFileTo("d:/tmp1")
initCode:
  - initFinalCacheFile("d:/cache")
  - finalCacheFile("/sql/show_databases.sql")
  - finalCacheFile("/sql/show_full_tables_from_testdb1.sql")
  - finalCacheFile("/sql/show_full_tables_from_testdb2.sql")
  - finalCacheFile("/sql/describe_testdb1_travelrecord.sql")
  - finalCacheFile("/sql/describe_testdb1_address.sql")
```



lib

引用函数库



schemaName

必须以 schema.table的格式填写库与表的关系,以,分隔,遇上只有库没有表的情况,请用*代替表名



initCode

填写全局的初始化函数,这些函数在启动的时候只会调用一次



sql:
  - sql: use {schema}
    code: useSchemaThenResponseOk(matcher.group("schema"))

一层sql包含多个匹配项,

二层sql是GPattern的模式,code中的代码是java1.5的代码



函数库与代码参考以下两个文档

29-mycat-gpattern.md

31-mycat-instructions.md



[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)

This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

------





