Mycat使用的线程模型是基于Reactor的设计模式，
先说几个概念:

1.NIOAcceptor,这个类继承于ProxyReactorThread， 在Reactor模式中扮演Acceptor与主Reactor角色，主要承担客户端的连接事件(accept)

2.MycatReactorThread, 同样继承于ProxyReactorThread，在acceptor监听客户端连接后，交于MycatReactorThread处理

3.ProxyReactorThread，NIOAcceptor和MycatReactorThread的父类，是一个继承了Thread的线程类

4.ProxyRuntime，我理解的为一个运行时容器

5.MycatSession，前端连接会话

6.MySQLCommand 用来向前段写数据,或者后端写数据的cmd

上一篇我们说到了MySQLCommand.
