Mycat使用的线程模型是基于Reactor的设计模式，
先说几个概念:
1.NIOAcceptor,这个类继承于ProxyReactorThread， 在Reactor模式中扮演Acceptor与主Reactor角色，主要承担客户端的连接事件(accept)
2.MycatReactorThread, 同样继承于ProxyReactorThread，在acceptor监听客户端连接后，交于MycatReactorThread处理
3.ProxyReactorThread，NIOAcceptor和MycatReactorThread的父类，是一个继承了Thread的线程类
4.ProxyRuntime，我理解的为一个运行时容器
5.MycatSession，前端连接会话


下面开始流程：
程序的入口是io.mycat.mycat2.MycatCore. 在main 方法中 首选取得ProxyRuntime的实例,该类是一个单例模式
初始化时:

runtime.setReactorThreads(new MycatReactorThread[cpus]);  设置MycatReactorThread的线程池数量，也就是有多少个Selector
runtime.setSessionManager(new MycatSessionManager()); 设置session为前台连接session
