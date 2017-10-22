# 前言

在mycat2.0-启动流程中我们了解到ProxyRuntime在启动时,通过调用该类的startHeartBeatScheduler方法,启动了heartbeatScheduler.那么heartbeatScheduler到底做了什么事?我们就来分析一下.

# 分析

1. 在io.mycat.proxy.ProxyRuntime#startHeartBeatScheduler中首先检测是否已有heartBeatTasks在执行,**目的是为了避免重复执行**.


2. 通过MycatConfig获得HeartbeatConfig对象,从而获得replicaHeartbeat ,**可通过heartbeat.yml进行配置,默认是10 * 1000 ms.** 然后在heartBeatTasks中进行保存.Key 为REPLICA_HEARTBEAT,value 为 ScheduledFuture.该任务每隔10 * 1000 ms 调用io.mycat.proxy.ProxyRuntime#replicaHeartbeat方法.代码如下:

	```
	heartBeatTasks.put(REPLICA_HEARTBEAT,	heartbeatScheduler.scheduleAtFixedRate(replicaHeartbeat(),0,replicaHeartbeat,TimeUnit.MILLISECONDS));
	
	```


3. 在replicaHeartbeat中,会随机的选择一个ProxyReactorThread,然后调用addNIOJob方法进行任务的提交.代码如下:

	```
		private Runnable replicaHeartbeat() {
			return ()->{
				ProxyReactorThread<?> reactor  = getReactorThreads()[ThreadLocalRandom.current().nextInt(getReactorThreads().length)];
				reactor.addNIOJob(()-> config.getMysqlRepMap().values().stream().forEach(f -> f.doHeartbeat()));
			};
		}
	```

4. addNIOJob方法最终将Runnable提交到ProxyReactorThread中的pendingJobs中,pendingJobs是一个ConcurrentLinkedQueue<Runnable>类型的队列.

5. ProxyReactorThread是个**线程**,在其复写的run方法中,会从pendingJobs拉取任务进行处理.代码如下:
    ```
    if (keys.isEmpty()) {
    	if (!pendingJobs.isEmpty()) {
    		ioTimes = 0;
    		this.processNIOJob();
    	}
    	continue;
    } else if ((ioTimes > 5) & !pendingJobs.isEmpty()) {
    	ioTimes = 0;
    	this.processNIOJob();
    }
    ```
 
      由此可见,该方法的调用时机有两处:
      1. 当没有IO事件处理并且pendingJobs有任务积压时.
      2. 当有IO事件处理时,是不会进行pendingJobs任务处理的,因此当如此这般超过5次后,会一次性的进行任务处理.
    
     **因此,当有大量IO事件进行处理时,会有大量的任务的积压,为了解决这个问题,因此才引入ioTimes计数器.**
     
    **注意,这里有个问题,这里使用的容器是ConcurrentLinkedQueue,是否会有并发的问题呢?
     即: 是否存在 1个线程不断的往里放,一个线程不断的取, 如果放的速度 大于 取得速度, 就会出现死循环的情况. 
     答案:不会.因为该方法只会处理链接.而连接速度是不会比nio线程快的，另外多个reactor对一个acceptor更不可能速度慢.==这里感谢little-pan的解惑==**


6. 在io.mycat.proxy.ProxyReactorThread#processNIOJob中的实现就很简单了,进行任务的拉取与提交.代码如下:


```
private void processNIOJob() {
	Runnable nioJob = null;
	while ((nioJob = pendingJobs.poll()) != null) {
		try {
			nioJob.run();
		} catch (Exception e) {
			logger.warn("run nio job err ", e);
		}
	}

}
```

7. 对于当前的场景,我们要处理的任务如下所示:

```
config.getMysqlRepMap().values().stream().forEach(f -> f.doHeartbeat())
```

8. 在io.mycat.mycat2.beans.MySQLRepBean#doHeartbeat方法中,只是很简单的判断了是否写节点存在,如果不存在,直接return.然后依次遍历MySQLMetaBean,调用其doHeartbeat方法.

    ```
    public void doHeartbeat() {
    if (metaBeans.get(writeIndex) == null) {
    	return;
    }
     for (MySQLMetaBean source : metaBeans) {
    	if (source != null) {
    		source.doHeartbeat();
    	} else {
    		StringBuilder s = new StringBuilder();
    		s.append(Alarms.DEFAULT).append(replicaBean.getName()).append(" current dataSource is null!");
    		logger.error(s.toString());
    	}
    }
    }

    ```

9.  在io.mycat.mycat2.beans.MySQLMetaBean#doHeartbeat中的处理如下:
    1. 检查当前时间是否小于heartbeatRecoveryTime(心跳暂停时间),如果是,return.
    2.  调用io.mycat.mycat2.beans.heartbeat.DBHeartbeat#heartbeat发送心跳.

代码如下:

```
public void doHeartbeat() {
	// 未到预定恢复时间，不执行心跳检测。
	if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
		return;
	}
	
	try {
		heartbeat.heartbeat();
	} catch (Exception e) {
		logger.error(dsMetaBean.getHostName() + " heartbeat error.", e);
	}
}
```

10. 
MySQLHeartbeat中主要有三个属性在该方法中用到,现说明一下:

    1.  ReentrantLock lock 防止并发问题.
    
    2.   MySQLDetector detector mysql探测器,就是通过它进行mysql的状态检测
    
    3. final AtomicBoolean isChecking = new AtomicBoolean(false) , 标志位 ,采用cas机制. 

在io.mycat.mycat2.beans.heartbeat.MySQLHeartbeat#heartbeat中主要做了以下几件事:
    
    1. 获得一把锁,防止出现并发问题.
    2. 如果isChecking 当前为false.
        1. 当 detector == null || detector.isQuit() 时,会重新初始化detector,并调用heartbeat方法.
        2. 如果不是,则直接调用heartbeat.
    3.  如果isChecking 为true并且detector不为null时
        1. 当detector为退出时,将detector置为false,已便下次进行心跳处理.
        2. 如果是心跳处理超时,则进入心跳超时处理逻辑.
    4.  释放锁.

**现就心跳处理正常时的流程进行分析**:

10.1 在io.mycat.mycat2.beans.heartbeat.MySQLDetector#heartbeat中,调用了io.mycat.proxy.MycatReactorThread#getMysqlSession. 用于心跳 时,获取可用连接.代码如下:


```
MycatReactorThread reactor = (MycatReactorThread)Thread.currentThread();

reactor.getMysqlSession(heartbeat.getSource(), (optSession, sender, exeSucces, rv) -> {...});
```


10.2 在io.mycat.proxy.MycatReactorThread#getMysqlSession中,做了如下处理:
 
10.2.1. 从当前ator其他mycatSession中获取连接(Session),并从中获得MySQLMetaBean的一个空闲连接.代码如下:
    
    
   
```
LinkedList<MycatSession> mycatSessions = getAllSessions();
	mysqlSession = mycatSessions.stream()
            .map(mycatSession->mycatSession.getMySQLSession(mySQLMetaBean))
            .filter(session -> session != null).findFirst().orElse(null);
```

    
10.2.1. 如果mysqlSession不为null,则首先io.mycat.mycat2.MycatSession#unbindBeckend方法.处理流程如下:


10.2.1.1 首先从MycatSession中的backendMap(类型为ConcurrentHashMap<MySQLRepBean, List<MySQLSession>>)获得List<MySQLSession>.如果MycatSession不为null,则调用MysqlSession#unbindMycatSession,并将改session从list中删除.


```
List<MySQLSession> list = backendMap.get(mysqlSession.getMySQLMetaBean().getRepBean());
if(list!=null){
	mysqlSession.unbindMycatSession();
	list.remove(mysqlSession);
}
```

10.2.1.1.1 在MySQLSession#unbindMycatSession中,实现很简单,共做了5件事:
    
    1.  将ProxyBuffer进行重置,并将referedBuffer标志位置为false.
    
    2.  将curBufOwner属性置为true,该标志位的做作是为了说明是否多个Session共用同一个Buffer时，当前Session是否暂时获取了Buffer独家使用权，即独占Buffer.

>     进一步的解释:前端连接对应的session是 MycatSession,后端连接对应的session 是mysqlSession,这里是为了说明前后端session.到底是谁拥有的buffer.因为在透传的时候，已经明确了是单工模式.(感谢李艳军进行进一步的说明)
    
    3.  将MycatSession置为null.
    
    4.  将cmdChain置为null.

    5.  从当前session绑定的sessionAttrMap中删除对于session_key_conn_idle_flag的设置. (标识当前连接的闲置状态标识 ，true，闲置，false，未闲置,即在使用中)
    
    
代码如下所示:


```
public void unbindMycatSession() {
this.useSharedBuffer(null);
this.setCurBufOwner(true); //设置后端连接 获取buffer 控制权
if(this.mycatSession != null) {
	this.mycatSession.clearBeckend(this);
}
this.mycatSession = null;
this.setCmdChain(null);
this.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
}
```

10.2.2 如果与当前会话绑定的后端链接不为null的话,将后端链接置为null.


```
public void clearBeckend(MySQLSession mysqlSession){
	if(curBackend!=null&&curBackend.equals(mysqlSession)){
		curBackend = null;
	}
}
```


10.2.3 进行回调.对于当前场景来说,做了如下操作:
    
10.2.3.1.  初始化BackendHeartbeatTask,当heartbeatTask处理完毕后,会将当前session的CurNIOHandler恢复为默认的Handler.代码如下所示:
    


```
BackendHeartbeatTask heartbeatTask = new BackendHeartbeatTask(optSession,detector);
heartbeatTask.setCallback((mysqlsession, sder, isSucc, rsmsg) -> {
	//恢复默认的Handler
	optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
	
});
```


10.2.3.2  将当前的optSession的CurNIOHandler,设为heartbeatTask.代码如下:


```
optSession.setCurNIOHandler(heartbeatTask);
```

10.2.3.3 调用BackendHeartbeatTask的doHeartbeat方法.在该方法中将HeartBeat要发送的语句封装到CommandPacket中,然后调用io.mycat.proxy.AbstractSession#writeToChannel 进行数据的写入.代码如下:


```
public void doHeartbeat(){
    optSession.proxyBuffer.reset();
	CommandPacket packet = new CommandPacket();
	packet.packetId = 0;
	packet.command = MySQLPacket.COM_QUERY;

	packet.arg = repBean.getReplicaBean().getRepType().getHearbeatSQL().getBytes();

	packet.write(optSession.proxyBuffer);
	optSession.proxyBuffer.flip();
	optSession.proxyBuffer.readIndex = optSession.proxyBuffer.writeIndex;
	try {
		optSession.writeToChannel();
	} catch (IOException e) {
		e.printStackTrace();
		logger.error(" The backend heartbeat task write to mysql is error . {}",e.getMessage());
		detector.getHeartbeat().setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
		detector.getHeartbeat().setResult(DBHeartbeat.ERROR_STATUS, detector,  null);
	}
}

```


>  注意: HeartBeat语句在不同的场景是不一样的。
1. SINGLE_NODE(单一节点)-->select 1
1. MASTER_SLAVE(普通主从)-->show slave status
1. GARELA_CLUSTER(普通基于garela cluster集群)-->show status like 'wsrep%'


10.3 如果mysqlSession == null 的话,则从ds中获取已经建立的连接,然后调用回调.



10.4 如果mysqlSession == null,并且之前没有建立的链接,则调用io.mycat.proxy.MycatReactorThread#createSession进行新建.
代码如下:


```
if(logger.isDebugEnabled()){
  logger.debug("create new connection ");
}

createSession(mySQLMetaBean, null, (optSession, Sender, exeSucces, retVal) -> {

  if (exeSucces) {
		//设置当前连接 读写分离属性
		optSession.setDefaultChannelRead(mySQLMetaBean.isSlaveNode());
		//恢复默认的Handler
		optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
  	callback.finished(optSession, null, true, null);
  } else {
  	callback.finished(optSession, null, false, retVal);
  }
});
```


10.4.1 统计reactor backend线程的使用数量和所有后端正在使用的连接数.代码如下:


```
// reactor backend线程的使用数量
int count = Stream.of(ProxyRuntime.INSTANCE.getReactorThreads())
.map(session -> ((MycatReactorThread)session).mySQLSessionMap.get(mySQLMetaBean))
.filter(list -> list != null)
.reduce(0, (sum, list) -> sum += list.size(), (sum1, sum2) -> sum1 + sum2);


// 所有后端正在使用的连接数
private int getUsingBackendConCounts(MySQLMetaBean mySQLMetaBean) {
return allSessions.stream()
.map(session -> {
	MycatSession mycatSession = (MycatSession) session;
	return mycatSession.getBackendConCounts(mySQLMetaBean);
})
.reduce(0, (sum, count) -> sum += count, (sum1, sum2) -> sum1 + sum2);
}
```
10.4.2 如果reactor backend线程的使用数量 + 所有后端正在使用的连接数 + 1 > 最大连接数的话,则进行回调,回调后执行的代码如下:


```
optSession.close(false, ((ErrorPacket)rv).message);
//连接创建 失败. 如果是主节点，需要重试.并在达到重试次数后,通知集群
if(heartbeat.incrErrorCount() < heartbeat.getSource().getDsMetaBean().getMaxRetryCount()){
heartbeat();
}else{
heartbeat.setResult(DBHeartbeat.ERROR_STATUS, 
this, 
heartbeat.getSource().getDsMetaBean().getIp()+":"+heartbeat.getSource().getDsMetaBean().getPort()
+" connection timeout!!");
}
```

10.4.2.1 关闭后端mysql链接.

10.4.2.2 如果通知失败次数 < 心跳失败最大次数的话(**默认是5次,可在datasource.yml中进行配置,配置项为replicas.mysqls.$mysql.maxRetryCount进行配置**),则进行重试,重新调用io.mycat.mycat2.beans.heartbeat.MySQLDetector#heartbeat.

10.4.2.3 否则,将此次心跳检测的结果置为false.


10.4.3 创建BackendConCreateTask进行后端链接的建立.

如果建立链接成功的话,则进行如下处理:

    1.  设置当前连接读写分离属性
    2.  将CurNIOHandler置为默认的Handler
    3.  调用10.2.3步.

如果执行失败的话,则执行10.4.2步.