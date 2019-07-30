

# MySQL报文解析状态机

author:chenjunwen 2019-7-22

<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.

mycat proxy使用自研的报文解析实现，基于一个最基本的代理基本原则，流式传输报文，在不需要把一个报文完整接收的原则上，把报文转发到从mysql客户端转发到mysql服务器，然后接收mysql服务器的响应转发到mysql客户端。

首先我们可以分析一下代理需要哪些特性，这些特性可能决定了需要对报文进行哪些处理，比如读写分离就需要把SQL接收完整然后分析是路由到主节点还是从节点。这就已经决定了请求是需要接收完毕的。但是对于预处理中的blob类型（longdata）可能直接往后端传递数据比较好，而不是都把数据先放在代理里面，再往后端发送，到底选用哪种实现方式，就要具体看后续怎样分析报文了。

一般来说，如果代理总是把前端连接与后端连接绑定，我们的实现很简单，就是把数据转发就可以了。但是采用连接复用技术，一个前端连接在没有事务的情况下，执行完成一个SQL就要把连接归还到连接池，这就要求代理需要准确判断响应是否结束。在有能把报文接收完整或者基于行迭代器的第三方数据库客户端作为后端连接支持下，这个判断响应结束的问题并不突出。在有事务的情况下，完全可以直接把后端的读取通道与前端的写入通道进行绑定，直到代理接收到指示事务结束的相关SQL才在此次响应之后检查是否解除绑定。

分析响应是否结束需要对报文进行分析，mysql报文其实有两类，一种是与元数据相关，一种是行数据。对于元数据相关的，它们一般指示了连接的状态以及响应的阶段，对于这些报文一般比较小，也是分析必须的数据，所以可以把它们完整接收即可。对于行数据，也可以能每列的数据也比较大，所以proxy里面的设计是在接收到mysql报文长度之后，在此长度之内报文就可以转发数据。这就引出了最后一个问题如何判断报文的类型，或者换一句话说，这个报文处于请求响应中的那个阶段？

要想知道报文的类型，需要把请求响应报文的流转进行分析，总体的架构就是在代理商构建一个请求响应处理的状态机。对请求进行分析，基于请求的上下文，再对响应进行分析，请求报文类型总是已知的，响应的第一个报文的类型总是可以被推断处理，然后就是根据报文的标记或者采用计数法分析报文的类型。对于报文没有拆分报文的结果集的情况，响应解析是很容易的，而且满足99%的业务系统场景，一般情况下实现这个报文解析就可以了。稍难一点的是多结果集的情况的响应。

```
reveive data...
while(response is not end){
	if type of payload need completely{
			read payload completely
			set current type of payload
			set whether response is end 
			converting data to other object or formats etc.
	}else {
			read packet partly
			set current type of payload when the end of payload
			set whether response is end when the payload in packet has reveived 					completely
			send part packet to front or store it etc.
	}
}
```

spliting packet

```
read data...
if the length of data >=4
	read the length of payload
		if the length of payload  < 0xffffff-1
        			not spliting packet
        if the length of payload  = 0xffffff-1
        			spliting packet
```



judge payload end

```
if the last packet is spliting and current packet is not
	payload spliting  end
if the last packet is spliting and current packet is spliting
	payload not end
if the last packet is not spliting and current packet is spliting
	payload not end(payload spliting first packet)
if the last packet is not spliting and current packet is not spliting
	payload end(payload not spliting first packet)
```



judge the type of payload

```
if packet is the first packet of payload
	read the first byte of payload to judge type of payload
if packet is not the first packet of packet
		read the first byte of payload,
		the length of payload(resolve the conflicting in the end of row)
		type of last payload(packet)(resolve the conflicting)
		maybe column count 
	
```



read packet partly

```
read data...
if remains bytes > 0
	remains bytes = remains bytes - the length of reveive payload
if the length of packet is not complete.
	remains bytes = the length of payload - the length of reveive payload
```



read payload completely

```
until the end packet data of payload is complete.
```

相关文档：100-mysql-packet-parsing-state-machine-en.md



------

