

# mycat 2.0-proxy(mycat.yml,代理)

author:junwen 2019-6-2 初始文档

author:junwen 2019-7-1 添加命令分发和IO buffer pool的说明

[![Creative Commons License](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)
This work is licensed under a [Creative Commons Attribution-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-sa/4.0/).

## mycat proxy配置

尽量描述不经常因为开发而变更的配置

```yaml
proxy:
  ip: 0.0.0.0
  port: 8066
  # 以下bufferPool是每个reactor1独立一个,不共享
  bufferPoolPageSize: 4194304     # 一页的大小，默认 1024*1024*4
  bufferPoolChunkSize: 8192       # chunk 大小 ， 默认 8192 
  bufferPoolPageNumber: 2        # 页数量. 默认 2
  reactorNumber: 2        #  默认 1 取逻辑处理器数量
  commandDispatcherClass: io.mycat.command.HybridCommandHandler
```

### ip

mycat proxy的绑定ip

### port

mycat proxy监听的端口

### commandDispatcherClass

自定义命令分发类,默认:   io.mycat.command.HybridCommandHandler

指定一个实现了CommandDispatcher接口的类即可

在proxy的设计中,并没有逻辑表,逻辑库等概念,这些概念的实现都是路由和mycat2主模块的构成的, 而mycat2提供的io.mycat.command.MycatCommandHandler就是CommandDispatcher的默认的实现,如果需要自定义路由的实现,替换该类即可



### reactorNumber

一个reactor对应一个代理处理线程,该线程的主要任务是对SQL进行非常简单的分析并转发流量,一般来说,一个reactor线程即可处理几千连接.



### IO缓冲池配置

一个线程只有一个缓冲池,一个缓冲池之间不被多个IO线程共享

一个缓冲池可以有多个页,一个页有一个固定的大小,而且可以分配固定大小(bufferPoolChunkSize)的堆外内存块

所有一个线程占用的堆外内存空间是

```java
bufferPoolPageNumber*bufferPoolPageSize
```

在多个reactor的情况下,进程占用的堆外内存空间的大小是

```java
reactorNumber*bufferPoolPageNumber*bufferPoolPageSize
```



------

