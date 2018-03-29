sql如何经过mycat到达mysql的
====
Mycat使用的线程模型是基于Reactor的设计模式，
先说几个概念:

1.NIOAcceptor,这个类继承于ProxyReactorThread， 在Reactor模式中扮演Acceptor与主Reactor角色，主要承担客户端的连接事件(accept)

2.MycatReactorThread, 同样继承于ProxyReactorThread，在acceptor监听客户端连接后，交于MycatReactorThread处理

3.ProxyReactorThread，NIOAcceptor和MycatReactorThread的父类，是一个继承了Thread的线程类

4.ProxyRuntime，我理解的为一个运行时容器

5.MycatSession，前端连接会话（client连mycat）

6.MySQLSession，后端连接会话（mycat连Mysql）

7.MySQLCommand 用来向前段写数据,或者后端写数据的cmd


下面开始流程：
程序的入口是io.mycat.mycat2.MycatCore. 在main 方法中 首选取得ProxyRuntime的实例,该类是一个单例模式
初始化时:

  	public static void main(String[] args) throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		//设置负责读取配置文件的类
		runtime.setConfig(new MycatConfig());
		
		//加载配置文件
		ConfigLoader.INSTANCE.loadCore();
		solveArgs(args);

		int cpus = Runtime.getRuntime().availableProcessors();
		runtime.setNioReactorThreads(cpus);
		runtime.setReactorThreads(new MycatReactorThread[cpus]);

		// runtime.setNioProxyHandler(new DefaultMySQLProxyHandler());
		// runtime.setNioProxyHandler(new DefaultDirectProxyHandler());
		// runtime.setSessionManager(new DefaultTCPProxySessionManager());
		// Debug观察MySQL协议用
		// runtime.setSessionManager(new MySQLStudySessionManager());
		runtime.setSessionManager(new MycatSessionManager());
		runtime.init();

		ProxyStarter.INSTANCE.start();
	}
我们展开ProxyStarter.INSTANCE.start();集群这里不做详细说明

	public void start() throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = runtime.getConfig();
		ProxyConfig proxyConfig = conf.getConfig(ConfigEnum.PROXY);
		ProxyBean proxybean = proxyConfig.getProxy();
		// 启动NIO Acceptor
		NIOAcceptor acceptor = new NIOAcceptor(new DirectByteBufferPool(proxybean.getBufferPoolPageSize(),
				proxybean.getBufferPoolChunkSize(),
				proxybean.getBufferPoolPageNumber()));
		acceptor.start();（1）
		runtime.setAcceptor(acceptor); （2）

		ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
		ClusterBean clusterBean = clusterConfig.getCluster();
		if (clusterBean.isEnable()) {
			// 启动集群
			startCluster(runtime, clusterBean, acceptor);
		} else {
			// 未配置集群，直接启动
			startProxy(true);
		}
	}
	
NIOAcceptor是一个线程，这里我们展开NIOAcceptor中的run方法

	public void run() {
		long ioTimes = 0;
		ReactorEnv reactorEnv = new ReactorEnv();
		while (true) {
			try {
				selector.select(SELECTOR_TIMEOUT);
				final Set<SelectionKey> keys = selector.selectedKeys();
				// logger.info("handler keys ,total " + selected);
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
				ioTimes++;
				for (final SelectionKey key : keys) {
					try {
						int readdyOps = key.readyOps();
						reactorEnv.curSession = null;
						// 如果当前收到连接请求
						if ((readdyOps & SelectionKey.OP_ACCEPT) != 0) {
							processAcceptKey(reactorEnv, key);
						}
						// 如果当前连接事件
						else if ((readdyOps & SelectionKey.OP_CONNECT) != 0) {
							this.processConnectKey(reactorEnv, key);
						} else if ((readdyOps & SelectionKey.OP_READ) != 0) {
							this.processReadKey(reactorEnv, key);

						} else if ((readdyOps & SelectionKey.OP_WRITE) != 0) {
							this.processWriteKey(reactorEnv, key);
						}
					} catch (Exception e) {
						logger.warn("Socket IO err :", e);
						key.cancel();
						if (reactorEnv.curSession != null) {
							reactorEnv.curSession.close(false, "Socket IO err:" + e);
							this.allSessions.remove(reactorEnv.curSession);
							reactorEnv.curSession = null;
						}
					}
				}
				keys.clear();
			} catch (IOException e) {
				logger.warn("caugh error ", e);
			}

		}

	}
(1)NIOAcceptor里面我们看到它通过一个死循环不断的监听事件,获取事件的超时时间为100ms.
如果没有事件要处理,并且pendingJobs不为空则进行任务处理.
如果ioTimes大于5并且pendingJobs不为空则进行任务处理.
否则就对事件进行处理.这里重点关注processAcceptKey(reactorEnv, key);我们发现processAcceptKey中调用了accept()方法。
	
	private void accept(ReactorEnv reactorEnv,SocketChannel socketChannel,ServerType serverType) throws IOException {
		// 找到一个可用的NIO Reactor Thread，交付托管
		ProxyReactorThread<?> nioReactor = getProxyReactor(reactorEnv);
		// 将通道注册到reactor对象上
		nioReactor.acceptNewSocketChannel(serverType, socketChannel);
	}
getProxyReactor这里可以理解为从工厂工获取一个可用的Reactor，这里我们的sessionManager为MycatSessionManager

	public void acceptNewSocketChannel(Object keyAttachement, final SocketChannel socketChannel) throws IOException {
		pendingJobs.offer(() -> {
			try {
				T session = sessionMan.createSession(keyAttachement, this.bufPool, selector, socketChannel, true);
				allSessions.add(session);
			} catch (Exception e) {
				logger.warn("regist new connection err " + e);
			}
		});
	}
	
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
这里有一个ConcurrentLinkedQueue的队列，在acceptNewSocketChannel添加，processNIOJob执行，同时，processNIOJob在run方法中一直死循环的执行。相当于阻塞。
展开createSession

	public MycatSession createSession(Object keyAttachment, BufferPool bufPool, Selector nioSelector,
			SocketChannel frontChannel, boolean isAcceptCon) throws IOException {
		logger.info("MySQL client connected  ." + frontChannel);
		MycatSession session = new MycatSession(bufPool, nioSelector, frontChannel);(1)
		// 第一个IO处理器为Client Authorware
		session.setCurNIOHandler(MySQLClientAuthHandler.INSTANCE);
		// 默认为透传命令模式
		//session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
		// 向MySQL Client发送认证报文
		session.sendAuthPackge();
		session.setSessionManager(this);
		allSessions.add(session);
		return session;
	}
我们将(1)展开会发现，在这里socketChannel注册到了nioReactor,并且为OP_READ,同时attach了当前的MycatSession

	public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		this(bufferPool, selector, channel, SelectionKey.OP_READ);

	}
	
	public AbstractSession(BufferPool bufferPool, Selector selector, SocketChannel channel, int socketOpt)
			throws IOException {
		this.bufPool = bufferPool;
		this.nioSelector = selector;
		this.channel = channel;
		InetSocketAddress clientAddr = (InetSocketAddress) channel.getRemoteAddress();
		this.addr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		this.host = clientAddr.getHostString();
		SelectionKey socketKey = channel.register(nioSelector, socketOpt, this);
		this.channelKey = socketKey;
		this.proxyBuffer = new ProxyBuffer(this.bufPool.allocate());
		this.sessionId = ProxyRuntime.INSTANCE.genSessionId();
		this.startTime =System.currentTimeMillis();
	}



(2)下面我们展开startProxy(true);
		
		public void startProxy(boolean isLeader) throws IOException {
				ProxyRuntime runtime = ProxyRuntime.INSTANCE;
				MycatConfig conf = runtime.getConfig();
				NIOAcceptor acceptor = runtime.getAcceptor();

				ProxyConfig proxyConfig = conf.getConfig(ConfigEnum.PROXY);
				ProxyBean proxyBean = proxyConfig.getProxy();
				if (acceptor.startServerChannel(proxyBean.getIp(), proxyBean.getPort(), ServerType.MYCAT)){（1）
					startReactor();（2）

					// 加载配置文件信息
					ConfigLoader.INSTANCE.loadAll();

					ProxyRuntime.INSTANCE.getConfig().initRepMap();
					ProxyRuntime.INSTANCE.getConfig().initSchemaMap();

					conf.getMysqlRepMap().forEach((repName, repBean) -> {
						repBean.initMaster();
						repBean.getMetaBeans().forEach(metaBean -> metaBean.prepareHeartBeat(repBean, repBean.getDataSourceInitStatus()));
					});
				}

				ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
				ClusterBean clusterBean = clusterConfig.getCluster();
				// 主节点才启动心跳，非集群按主节点处理
				if (isLeader) {
					runtime.startHeartBeatScheduler();
				}

				BalancerConfig balancerConfig = conf.getConfig(ConfigEnum.BALANCER);
				BalancerBean balancerBean = balancerConfig.getBalancer();
				// 集群模式下才开启负载均衡服务
			if (clusterBean.isEnable() && balancerBean.isEnable()) {
					runtime.getAcceptor().startServerChannel(balancerBean.getIp(), balancerBean.getPort(), ServerType.LOAD_BALANCER);
			}
		}
 (1) startServerChannel方法这里根据serverType获取不同的serverChannel，startServerChannel中的 openServerChannel这个方法的作用,将获取到的  serverChannel注册到selector，selector在ProxyReactorThread中定义，并注册为OP_ACCEPT。
 
 		serverChannel.register(selector, SelectionKey.OP_ACCEPT, serverType);
		
 （2）startReactor

	private void startReactor() throws IOException {
		// Mycat 2.0 Session Manager
		MycatReactorThread[] nioThreads = (MycatReactorThread[]) MycatRuntime.INSTANCE.getReactorThreads();
		ProxyConfig proxyConfig = ProxyRuntime.INSTANCE.getConfig().getConfig(ConfigEnum.PROXY);
		int cpus = nioThreads.length;
		
		for (int i = 0; i < cpus; i++) {
			MycatReactorThread thread = new MycatReactorThread(ProxyRuntime.INSTANCE.getBufferPoolFactory().getBufferPool());
			thread.setName("NIO_Thread " + (i + 1));
			thread.start();
			nioThreads[i] = thread;
		}
	}
MycatReactorThread和NIOAcceptor一样继承与ProxyReactorThread，这里创建了和CPU个数相同的线程组。并开启，上面中给我们说到，MycatSession在创建的时候注册到nioReactor,并监听读。这里我们看一下MycatReactorThread读的操作。

	protected void processReadKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		// only from cluster server socket
		T session = (T) curKey.attachment();
		reactorEnv.curSession = session;
		session.getCurNIOHandler().onSocketRead(session);
	}
这里的session为MycatSession,因为在注册的时候attach了。这里的CurNIOHandler为上文的MySQLClientAuthHandler。我们展开onSocketRead这个方法
	
	public void onSocketRead(MycatSession session) throws IOException {
		ProxyBuffer frontBuffer = session.getProxyBuffer();
		if (session.readFromChannel() == false
				|| CurrPacketType.Full != session.resolveMySQLPackage(frontBuffer, session.curMSQLPackgInf, false)) {
			return;
		}

		// 处理用户认证报文
		try {
			AuthPacket auth = new AuthPacket();
			auth.read(frontBuffer);

			MycatConfig config = ProxyRuntime.INSTANCE.getConfig();
			UserConfig userConfig = config.getConfig(ConfigEnum.USER);
			UserBean userBean = null;
			for (UserBean user : userConfig.getUsers()) {
				if (user.getName().equals(auth.user)) {
					userBean = user;
					break;
				}
			}

			// check user
			if (!checkUser(session, userConfig, userBean)) {
				failure(session, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "' with addr '" + session.addr + "'");
				return;
			}

			// check password
			if (!checkPassword(session, userBean, auth.password)) {
				failure(session, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "', because password is error ");
				return;
			}

			// check schema
			switch (checkSchema(userBean, auth.database)) {
				case ErrorCode.ER_BAD_DB_ERROR:
					failure(session, ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + auth.database + "'");
					break;
				case ErrorCode.ER_DBACCESS_DENIED_ERROR:
					String s = "Access denied for user '" + auth.user + "' to database '" + auth.database + "'";
					failure(session, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
					break;
				default:
					// set schema
					if (auth.database == null) {
						session.schema = (userBean.getSchemas() == null) ?
								config.getDefaultSchemaBean() : config.getSchemaBean(userBean.getSchemas().get(0));
					} else {
						session.schema = config.getSchemaBean(auth.database);
					}

					logger.debug("set schema: {} for user: {}", session.schema, auth.user);
					if (success(session, auth)) {
						session.clientUser=auth.user;//设置session用户
						session.proxyBuffer.reset();
						session.answerFront(AUTH_OK);
						// 认证通过，设置当前SQL Handler为默认Handler
						session.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);(1)
					}
			}
		} catch (Throwable e) {
			logger.warn("Frontend FrontendAuthenticatingState error:", e);
		}
	}
代码很长这里我们只看最后的session.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);假设已经认证成功的情况下，
DefaultMycatSessionHandler中的onSocketRead，这里session为MycatSession执行所以执行onFrontRead。

	public void onSocketRead(final AbstractMySQLSession session) throws IOException {
		if (session instanceof MycatSession) {
			onFrontRead((MycatSession) session);
		} else {
			onBackendRead((MySQLSession) session);
		}
	}

	private void onFrontRead(final MycatSession session) throws IOException {
		boolean readed = session.readFromChannel();
		ProxyBuffer buffer = session.getProxyBuffer();
		// 在load data的情况下，SESSION_PKG_READ_FLAG会被打开，以不让进行包的完整性检查
		if (!session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_PKG_READ_FLAG.getKey())
				&& readed == false) {
			return;
		}

		switch (session.resolveMySQLPackage(buffer, session.curMSQLPackgInf, false)) {
		case Full:
			session.changeToDirectIfNeed();
			break;
		case LongHalfPacket:
			// 解包获取包的数据长度
			int pkgLength = session.curMSQLPackgInf.pkgLength;
			ByteBuffer bytebuffer = session.proxyBuffer.getBuffer();
			if (pkgLength > bytebuffer.capacity() && !bytebuffer.hasRemaining()) {
				try {
					session.ensureFreeSpaceOfReadBuffer();
				} catch (RuntimeException e1) {
					if (!session.curMSQLPackgInf.crossBuffer) {
						session.curMSQLPackgInf.crossBuffer = true;
						session.curMSQLPackgInf.remainsBytes = pkgLength
								- (session.curMSQLPackgInf.endPos - session.curMSQLPackgInf.startPos);
						session.sendErrorMsg(ErrorCode.ER_UNKNOWN_ERROR, e1.getMessage());
					}
					session.proxyBuffer.readIndex = session.proxyBuffer.writeIndex;
				}
			}
		case ShortHalfPacket:
			session.proxyBuffer.readMark = session.proxyBuffer.readIndex;
			return;
		}

		if (session.curMSQLPackgInf.endPos < buffer.writeIndex) {
			logger.warn("front contains multi package ");
		}

		// 进行后端的结束报文处理的绑定
		CommandHandler adapter = HandlerParse.INSTANCE.getHandlerByType(session.curMSQLPackgInf.pkgType);(1)
		if (null == adapter) {
			logger.error("curr pkg Type :" + session.curMSQLPackgInf.pkgType + " is not handler proess");
			throw new IOException("curr pkgtype " + session.curMSQLPackgInf.pkgType + " not handler!");
		}

		// 指定session中的handler处理为指定的handler
		session.commandHandler = adapter;

		if (!session.matchMySqlCommand()) {(1)
			return;
		}

		// 如果当前包需要处理，则交给对应方法处理，否则直接透传
		if (session.curSQLCommand.procssSQL(session)) {
			session.curSQLCommand.clearFrontResouces(session, session.isClosed());
		}
	}
这里我们重点看一下CommandHandler adapter = HandlerParse.INSTANCE.getHandlerByType(session.curMSQLPackgInf.pkgType);

根据前台发过来的数据包类型选择不同的CommandHandler

这里有一个重要的方法       session.matchMySqlCommand(),      根据sql类型构建CmdChain.绑定MySqlCommand，我们展开来看看
	
	public boolean matchMySqlCommand(){
		switch(schema.schemaType){
			case DB_IN_ONE_SERVER:
				return DBInOneServerCmdStrategy.INSTANCE.matchMySqlCommand(this);
			case DB_IN_MULTI_SERVER:
				DBINMultiServerCmdStrategy.INSTANCE.matchMySqlCommand(this);
			case ANNOTATION_ROUTE:
				AnnotateRouteCmdStrategy.INSTANCE.matchMySqlCommand(this);
			//case SQL_PARSE_ROUTE:
			//AnnotateRouteCmdStrategy.INSTANCE.matchMySqlCommand(this);
			default:
				throw new InvalidParameterException("schema type is invalid ");
		}
	}
schemaType可在schema.yml中进行配置,默认是DB_IN_ONE_SERVER
我们这里只考虑DB_IN_ONE_SERVER，展开DBInOneServerCmdStrategy.INSTANCE.matchMySqlCommand(this);

	final public boolean matchMySqlCommand(MycatSession session) {
		
		MySQLCommand  command = null;
		if(MySQLPacket.COM_QUERY==(byte)session.curMSQLPackgInf.pkgType){
			/**
			 * sqlparser
			 */
			BufferSQLParser parser = new BufferSQLParser();
			int rowDataIndex = session.curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize +1 ;
			int length = session.curMSQLPackgInf.pkgLength -  MySQLPacket.packetHeaderSize - 1 ;
			try {
				parser.parse(session.proxyBuffer.getBuffer(), rowDataIndex, length, session.sqlContext);
			} catch (Exception e) {
				try {
					logger.error("sql parse error",e);
					session.sendErrorMsg(ErrorCode.ER_PARSE_ERROR, "sql parse error : "+e.getMessage());
				} catch (IOException e1) {
					session.close(false, e1.getMessage());
				}
				return false;
			}
			
			byte sqltype = session.sqlContext.getSQLType()!=0?session.sqlContext.getSQLType():session.sqlContext.getCurSQLType();
			
			if(BufferSQLContext.MYCAT_SQL==sqltype){
				session.curSQLCommand = MyCatCmdDispatcher.INSTANCE.getMycatCommand(session.sqlContext);
				return true;
			}
			
			command = MYSQLCOMMANDMAP.get(sqltype);
		}else{
			command = MYCOMMANDMAP.get((byte)session.curMSQLPackgInf.pkgType);
		}
		if(command==null){
			command = DirectPassthrouhCmd.INSTANCE;
		}

		/**
		 * 设置原始处理命令
		 * 1. 设置目标命令
		 * 2. 处理动态注解
		 * 3. 处理静态注解
		 * 4. 构建命令或者注解链。    如果没有注解链，直接返回目标命令
		 */
		SQLAnnotationChain chain = new SQLAnnotationChain();
		session.curSQLCommand = chain.setTarget(command) 
			 .processDynamicAnno(session)
			 .processStaticAnno(session, staticAnnontationMap)
			 .build();
		return true;
	}

这个方法首先根据mysql的报文类型生成MySQLCommand.如果是COM_QUERY,则调用BufferSQLParser进行解析.否则通过MYCOMMANDMAP生成MySQLCommand.最后,如果command等于NULL,则为DirectPassthrouhCmd.注解这块，不做详细分析。
由于当前为DirectPassthrouhCmd，即session.curSQLCommand=DirectPassthrouhCmd, 那我们就看一下DirectPassthrouhCmd中的procssSQL
	
	public boolean procssSQL(MycatSession session) throws IOException {
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		session.clearReadWriteOpts();（1）

		session.getBackend((mysqlsession, sender, success, result) -> {

			ProxyBuffer curBuffer = session.proxyBuffer;
			// 切换 buffer 读写状态
			curBuffer.flip();
			if (success) {
				// 没有读取,直接透传时,需要指定 透传的数据 截止位置
				curBuffer.readIndex = curBuffer.writeIndex;
				// 改变 owner，对端Session获取，并且感兴趣写事件
				session.giveupOwner(SelectionKey.OP_WRITE);
				try {
					mysqlsession.writeToChannel();
				} catch (IOException e) {
					session.closeBackendAndResponseError(mysqlsession, success, ((ErrorPacket) result));
				}
			} else {
				session.closeBackendAndResponseError(mysqlsession, success, ((ErrorPacket) result));
			}
		});
		return false;
	}
这个方法步骤
1.首先取消前端读写事件，因为获取后端连接可能涉及到异步处理

2.调用session.getBackend获取一个后端（mysql端）连接，并将命令发送给后端，我们看下getBackend

	public void getBackend(AsynTaskCallBack<MySQLSession> callback) throws IOException {
		MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
		
		final boolean runOnSlave = canRunOnSlave();
		
		MySQLRepBean repBean = getMySQLRepBean(getbackendName());
		
		/**
		 * 本次根据读写分离策略要使用的metaBean
		 */
		MySQLMetaBean targetMetaBean = repBean.getBalanceMetaBean(runOnSlave);
		
		if(targetMetaBean==null){
			String errmsg = " the metaBean is not found,please check datasource.yml!!! [balance] and [type]  propertie or see debug log or check heartbeat task!!";
			if(logger.isDebugEnabled()){
				logger.error(errmsg);
			}
			ErrorPacket error = new ErrorPacket();
            error.errno = ErrorCode.ER_BAD_DB_ERROR;
            error.packetId = 1;
            error.message = errmsg;
			responseOKOrError(error);
			return;
		}
首先获取当前线程MycatReactorThread，根据 backendName获取MySQLRepBean(一組MySQL复制集群，如主从或者多主)(backendNam可在schema.yml中设置schemas.defaultDN.replica)
然后根据=读写分离策略找出要使用的metaBean，如果为null，返回错误。

2.canRunOnSlave方法判断后端连接 是否可以走从节点
 静态注解情况下 走读写分离
 事务场景下，走从节点
 
 	private boolean canRunOnSlave(){
		 //静态注解情况下 走读写分离
		if(NewSQLContext.ANNOTATION_BALANCE==sqlContext.getAnnotationType()){
			final long balancevalue = sqlContext.getAnnotationValue(NewSQLContext.ANNOTATION_BALANCE);
			if(TokenHash.MASTER == balancevalue){
				return false;
			}else if(TokenHash.SLAVE == balancevalue){
				return true;
			}else{
				logger.error("sql balance type is invalid, run on slave [{}]",sqlContext.getRealSQL(0));
			}
			return true;
		}

		 //非事务场景下，走从节点
		if(AutoCommit.ON ==autoCommit){
			if(masterSqlList.contains(sqlContext.getSQLType())){
				return false;
			}else{
				//走从节点
				return true;
			}
		}else{
			return false;
		}
	}
	
展开getBalanceMetaBean方法

	public MySQLMetaBean getBalanceMetaBean(boolean runOnSlave){
		if(ReplicaBean.RepTypeEnum.SINGLE_NODE == replicaBean.getRepType()||!runOnSlave){
			return getCurWriteMetaBean();
		}

		MySQLMetaBean datas = null;

			switch(replicaBean.getBalanceType()){
				case BALANCE_ALL:
					datas = getLBReadWriteMetaBean();
					break;
				case BALANCE_ALL_READ:
					datas = getLBReadMetaBean();
					//如果从节点不可用,从主节点获取连接
					if(datas==null){
						logger.warn("all slaveNode is Unavailable. use master node for read . balance type is {}", replicaBean.getBalanceType());
						datas = getCurWriteMetaBean();
					}
					break;
				case BALANCE_NONE:
					datas = getCurWriteMetaBean();
					break;
				default:
					logger.warn("current balancetype is not supported!! [{}], use writenode connection .", replicaBean.getBalanceType());
					datas = getCurWriteMetaBean();
					break;
			}
			return datas;
	    }

默认配置的是BALANCE_ALL_READ,因此会调用getLBReadMetaBean,如果从节点不可用,则从主节点获取连接，这几种情况可展开单独说明，此处跳过
我们来看一下 getLBReadMetaBean

	private MySQLMetaBean getLBReadMetaBean(){
	List<MySQLMetaBean> result = metaBeans.stream()
			.filter(f -> f.isSlaveNode() && f.canSelectAsReadNode())
			.collect(Collectors.toList());
	return result.isEmpty() ? null : result.get(ThreadLocalRandom.current().nextInt(result.size()));
	}

这个方法是去查找去读节点，放入list并返回。
继续展开canSelectAsReadNode方法

	public boolean canSelectAsReadNode() {
		int slaveBehindMaster = heartbeat.getSlaveBehindMaster();
		int dbSynStatus = heartbeat.getDbSynStatus();
		
		if (!isAlive()){
			return false;
		}
		
		if (dbSynStatus == DBHeartbeat.DB_SYN_ERROR) {
			return false;
		}
		boolean isSync = dbSynStatus == DBHeartbeat.DB_SYN_NORMAL;
		boolean isNotDelay = (slaveThreshold >= 0) ? (slaveBehindMaster < slaveThreshold) : true;
		return isSync && isNotDelay;
	}
这个方法主要检查当前节点是否可用。如果不可用
走getCurWriteMetaBean()方法。展开getCurWriteMetaBean()

	private MySQLMetaBean getCurWriteMetaBean() {
	return metaBeans.get(writeIndex).isAlive() ? metaBeans.get(writeIndex) : null;
	}
如果当前读不可用，得到写节点。从写节点读取数据

接下来我们看下session.sendAuthPackge();

	public void sendAuthPackge() throws IOException {
		// 生成认证数据
		byte[] rand1 = RandomUtil.randomBytes(8);
		byte[] rand2 = RandomUtil.randomBytes(12);

		// 保存认证数据
		byte[] seed = new byte[rand1.length + rand2.length];
		System.arraycopy(rand1, 0, seed, 0, rand1.length);
		System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
		this.seed = seed;

		// 发送握手数据包
		HandshakePacket hs = new HandshakePacket();
		hs.packetId = 0;
		hs.protocolVersion = Version.PROTOCOL_VERSION;
		hs.serverVersion = Version.SERVER_VERSION;
		hs.threadId = this.getSessionId();
		hs.seed = rand1;
		hs.serverCapabilities = getServerCapabilities();
		// hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
		hs.serverStatus = 2;
		hs.restOfScrambleBuff = rand2;
		hs.write(proxyBuffer);
		// 设置frontBuffer 为读取状态
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		this.writeToChannel();(1)
	}
这里我们只看最后一行，这里是proxyBuffer写入channel中，我们把它展开

	public void writeToChannel() throws IOException {
		checkBufferOwner(true);
		ByteBuffer buffer = proxyBuffer.getBuffer();
		buffer.limit(proxyBuffer.readIndex);
		buffer.position(proxyBuffer.readMark);
		int writed = channel.write(buffer);
		proxyBuffer.readMark += writed; // 记录本次磁轭如到 Channel 中的数据
		if (!buffer.hasRemaining()) {
			// logger.debug("writeToChannel write  {} bytes ,curChannel is {}", writed,this);
			// buffer 中需要透传的数据全部写入到 channel中后,会进入到当前分支.这时 readIndex == readLimit
			if (proxyBuffer.readMark != proxyBuffer.readIndex) {
				logger.error("writeToChannel has finished but readIndex != readLimit, please fix it !!!");
			}
			if (proxyBuffer.readIndex > buffer.capacity() * 2 / 3) {
				proxyBuffer.compact();
			} else {
				buffer.limit(buffer.capacity());
			}
			// 切换读写状态
			// proxyBuffer.flip();
			/*
			 * 如果需要自动切换owner,进行切换 1. writed==0 或者 buffer 中数据没有写完时,注册可写事件
			 * 时,会进行owner 切换 注册写事件,完成后,需要自动切换回来
			 */
			// if (proxyBuf.needAutoChangeOwner()) {
			// proxyBuf.changeOwner(!proxyBuf.frontUsing()).setPreUsing(null);
			// }
		} else {
			/**
			 * 1. writed==0 或者 buffer 中数据没有写完时,注册可写事件 通常发生在网络阻塞或者 客户端
			 * COM_STMT_FETCH 命令可能会 出现没有写完或者 writed == 0 的情况
			 */
			logger.debug("register OP_WRITE  selectkey .write  {} bytes. current channel is {}", writed, channel);
			// 需要切换 owner ,同时保存当前 owner 用于数据传输完成后,再切换回来
			// proxyBuf 读写状态不切换,会切换到相同的事件,不会重复注册
			// proxyBuf.setPreUsing(proxyBuf.frontUsing()).changeOwner(!proxyBuf.frontUsing());
		}
		checkWriteFinished();
	}

这里设计的很巧妙，
1.proxyBuffer因为不能同时进行读写，所以确保proxyBuffer是可读状态。

2.channel 始终从 readMark 开始 读取数据，到 readIndex 结束。
   即：写入到 channel中的数据范围是 readMark---readIndex 之间的数据。
   
3. readMark 指针的移动
   将数据写出到channel中后,readMark 对应写出了多少数据。即： writed = channel.write(buffer);
   每次写出数据后，readMark 增加写出数据的长度。即： readMark += writed ;
   readMark默认值为0. 有可能存在 要写出的数据 writed 没有写出去,或者只写出去了一部分的情况。
   下次channel 可写时（通常可写事件被触发），接着从readMark 开始写出数据到channel中。
   当readMark==readIndex 时,代表 数据全部写完。
   
4. 读写状态转换
   数据全部写完后,proxybuffer 状态 转换为 可写状态。即  inReading = false;
   
5. proxybuffer 压缩。
   每次从proxybuffer读取数据写入到channel后，
   判断当前proxybuffer 已读是否大于总容量的2/3（readIndex > buffer.capacity() * 2 / 3).
   如果大于 2/3 进行一次 compact。
   
最后还有一个重要的方法checkWriteFinished，进行是否写入完毕检查
	
	protected void checkWriteFinished() throws IOException {
		checkBufferOwner(true);
		if (!this.proxyBuffer.writeFinished()) {
			this.change2WriteOpts();
		} else {
			writeFinished();
			// clearReadWriteOpts();
		}
	}
如果写入未完成，重新注册为OP_WRITE，重新注册到selector中，因为selector一直在轮询会在一次的执行writeToChannel。 继续执行写操作。
	
	public void change2WriteOpts() {
		checkBufferOwner(true);
		int intesOpts = this.channelKey.interestOps();
		// 事件转换时,只注册一个事件,存在可读事件没有取消注册的情况。这里把判断取消
		//if ((intesOpts & SelectionKey.OP_WRITE) != SelectionKey.OP_WRITE) {
		channelKey.interestOps(SelectionKey.OP_WRITE);
		//}
	}
如果写入完成

	public void writeFinished() throws IOException {
		this.getCurNIOHandler().onWriteFinished(this);

	}
因为已经通过认证这里的curNIOHandler为DefaultMycatSessionHandler

	public void onWriteFinished(AbstractMySQLSession session) throws IOException {
		// 交给SQLComand去处理
		if (session instanceof MycatSession) {
			MycatSession mycatSs = (MycatSession) session;
			if (mycatSs.curSQLCommand.onFrontWriteFinished(mycatSs)) {
				mycatSs.curSQLCommand.clearFrontResouces(mycatSs, false);
			}
		} else {
			MycatSession mycatSs = ((MySQLSession) session).getMycatSession();
			if (mycatSs.curSQLCommand.onBackendWriteFinished((MySQLSession) session)) {
				mycatSs.curSQLCommand.clearBackendResouces((MySQLSession) session, false);
			}
		}
	}
