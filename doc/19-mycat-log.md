# mycat 2.0 log

author:junwen 2019-7-1



mycat2使用固定的文件名记录访问的SQL以及资源使用情况

```
mycat-res.log 
```

记录资源使用情况



```
mycat-sql.log
```

记录原始的SQL以及发送到mysql的dataNode以及修改后的SQL



```
mycat-replica-indexes.log
```

记录主从切换记录



mycat2拥有日志记录器的封装接口,在实现上日志使用slf4j作为实现,与slf4j这些第三方日志工具不同之处是mycat日志记录器在记录日志的同时还可以生成错误报文,该错误报文可以作为响应.

在生成错误报文的错误码上,暂时没有统一的错误码规划.mycat作为mysql路由代理,其错误码并不能与mysql错误码一一对应,所以可以都使用ER_UNKNOWN_ERROR作为错误码.考虑到开发中的代码在不断变化,给每个日志赋予一个错误码的工作只能在模块稳定之后进行.

mycat2日志并没有使用i18n,统一使用英文作为日志语言.

日志字符串模板并没集中放置在一个类,而是分散到程序代码中.但是可以通过工具扫描代码中的日志代码,帮助生成用户日志文档.如果无法从该文档看出日志是什么意义,就要考虑改善该日志了,这将是一个长期的工作.

在开发中日志输出的代码应该有以下格式,以配合日志扫描程序

每个日志模板应该在一行里写完整,不能使用+拼接字符串,也不能把日志模板作为常量.

```java
warn("log");//ok
info("log");//ok
error("log");//ok
debug("log");//ok
errorPacket("log");//ok and return a payload of error pakcet 

warn("log {} {} {}",1,2,3);//ok
info("log {} {} {}",1,2,3);//ok
error("log {} {} {}",1,2,3");//ok
debug("log {} {} {}",1,2,3);//ok
errorPacket("log {} {} {}",1,2,3);//ok and return a payload of error pakcet 
errorPacket(123,"log {} {} {}",1,2,3);//ok and return a payload of error pakcet 
new MycatException("error!");//ok and throw a exception;
new MycatException(123,"error!");//ok and throw a exception;
            
   
```

错误例子

```java
warn("log"+"log2");//error and collect warn("log" 
warn(LOG_MESSAGE);//error,LOG_MESSAGE is a variable
```













































































[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

