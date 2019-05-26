# Mycat 2

## configuration 

### mycat.yaml

| key                  |         |                                                              |
| -------------------- | ------- | ------------------------------------------------------------ |
| IP                   | 0.0.0.0 | localhost                                                    |
| port                 | 8066    | listen port                                                  |
| bufferPoolPageSize   | 4194304 | a page size of buffer,default:1024*1024*4                    |
| bufferPoolChunkSize  | 8192    | chunk  size,the base size allocated by bufferBool,not be too small, otherwise, the construction of the message will fail. |
| bufferPoolPageNumber | 64      | the number of page                                           |



## package

```
cd mycat2
compile assembly:single
```

or you can

set the Working directory to mycat2  and

set command line to compile assembly:single

in your run/debug configuration.



## run/debug

path to the configuration file(resources) as MYCAT_HOME added to VM options.

```
java -Dfile.encoding=UTF-8 -DMYCAT_HOME=D:\xxxxxxx -jar mycat2-0.1.jar 
```







## License

GPLv2