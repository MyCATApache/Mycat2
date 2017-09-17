package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.Interceptor.Interceptor;
import io.mycat.mycat2.Interceptor.impl.BlockSQLIntercepor;
import io.mycat.mycat2.Interceptor.impl.DefaultIntercepor;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mycat2.cmds.strategy.AnnotateRouteCmdStrategy;
import io.mycat.mycat2.cmds.strategy.DBINMultiServerCmdStrategy;
import io.mycat.mycat2.cmds.strategy.DBInOneServerCmdStrategy;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ErrorCode;
import io.mycat.util.RandomUtil;

/**
 * 前端连接会话
 *
 * @author wuzhihui
 *
 */
public class MycatSession extends AbstractMySQLSession {

	private static Logger logger = LoggerFactory.getLogger(MycatSession.class);

	private MySQLSession curBackend;

	//所有处理cmd中,用来向前段写数据,或者后端写数据的cmd的
	public MyCommand curSQLCommand;

	public BufferSQLContext sqlContext = new BufferSQLContext();

	/**
	 * Mycat Schema
	 */
	public SchemaBean schema;

	private ConcurrentHashMap<MySQLRepBean, List<MySQLSession>> backendMap = new ConcurrentHashMap<>();

	private static List<Byte> masterSqlList = new ArrayList<>();

	private Map<Interceptor,MyCommand> SQLCommands = new HashMap<>();
	
	/*拦截器列表 用来判断某个cmd是否需要加入的SQLCommands 进行系列的处理*/
	public static List<Interceptor> interceptorList = new ArrayList<>();
	
	static{
		masterSqlList.add(NewSQLContext.INSERT_SQL);
		masterSqlList.add(NewSQLContext.UPDATE_SQL);
		masterSqlList.add(NewSQLContext.DELETE_SQL);
		masterSqlList.add(NewSQLContext.REPLACE_SQL);
//		masterSqlList.add(NewSQLContext.SELECT_INTO_SQL);
//		masterSqlList.add(NewSQLContext.SELECT_FOR_UPDATE_SQL);
		masterSqlList.add(NewSQLContext.LOAD_SQL);
		masterSqlList.add(NewSQLContext.CALL_SQL);
		masterSqlList.add(NewSQLContext.TRUNCATE_SQL);

//		masterSqlList.add(NewSQLContext.BEGIN_SQL);
//		masterSqlList.add(NewSQLContext.START_SQL);
//		masterSqlList.add(NewSQLContext.SET_AUTOCOMMIT_SQL);
		
		interceptorList.add(BlockSQLIntercepor.INSTANCE);
		interceptorList.add(DefaultIntercepor.INSTANCE);
	}
	
	
	public void clearSQLCmdMap() {
		SQLCommands.clear();
	}
	/**
	 * 获取sql 类型
	 * @return
	 */
	public MyCommand getMyCommand(){
		switch(schema.type){
			case DB_IN_ONE_SERVER:
				return DBInOneServerCmdStrategy.INSTANCE.getMyCommand(this);
			case DB_IN_MULTI_SERVER:
				return DBINMultiServerCmdStrategy.INSTANCE.getMyCommand(this);
			case ANNOTATION_ROUTE:
				return AnnotateRouteCmdStrategy.INSTANCE.getMyCommand(this);
			case SQL_PARSE_ROUTE:
				return AnnotateRouteCmdStrategy.INSTANCE.getMyCommand(this);
			default:
				throw new InvalidParameterException("schema type is invalid ");
		}
	}

	public MycatSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) throws IOException {
		super(bufPool, nioSelector, frontChannel);

	}

	protected int getServerCapabilities() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		// boolean usingCompress = MycatServer.getInstance().getConfig()
		// .getSystem().getUseCompression() == 1;
		// if (usingCompress) {
		// flag |= Capabilities.CLIENT_COMPRESS;
		// }
		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= ServerDefs.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		return flag;
	}

	/**
	 * 给客户端（front）发送认证报文
	 *
	 * @throws IOException
	 */
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
		this.writeToChannel();
	}

	public int getBackendConCounts(MySQLMetaBean metaBean) {
		return (int)backendMap.values()
			.stream()
			.flatMap(f->f.stream())
			.filter(f->f.getMySQLMetaBean().equals(metaBean))
			.count();
	}

	/**
	 * 绑定后端MySQL会话
	 *
	 * @param backend
	 */
	public void bindBackend(MySQLSession backend) {
		this.curBackend = backend;
		/*
		 * 这里，不能reset . 
		 * 一个前端连接, 的sessionM安排 中有多个后端连接时,  多个后端连接和前端连接使用的是同一个buffer.
		 * 这里reset ,会把前端连接的buffer 也给reset的掉.
		 * 连接池  新创建的连接放入 reactor 时,会进行一次reset ,保证  session 拿到的连接 buffer 状态是正确的.
		 */
//		backend.proxyBuffer.reset();
		putbackendMap(backend);
		backend.setMycatSession(this);
		backend.useSharedBuffer(this.proxyBuffer);
		backend.setCurNIOHandler(this.getCurNIOHandler());
	}

	/**
	 * 将所有后端连接归还到ds中
	 */
	public void unbindAllBackend() {
		final MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
		backendMap.forEach((key, value) -> {
			if (value != null) {
				value.forEach(mySQLSession -> {
					mySQLSession.unbindMycatSession();
					reactor.addMySQLSession(mySQLSession.getMySQLMetaBean(), mySQLSession);
				});
			}
		});
		backendMap.clear();
	}
	
	public void unbindBeckend(MySQLSession mysqlSession){
		List<MySQLSession> list = backendMap.get(mysqlSession.getMySQLMetaBean().getRepBean());
		if(list!=null){
			mysqlSession.unbindMycatSession();
			list.remove(mysqlSession);
		}
		if(curBackend!=null&&curBackend.equals(mysqlSession)){
			curBackend = null;
		}
	}
	
	/**
	 * 解除绑定当前 metaBean 所有的后端连接
	 * @param mySQLMetaBean
	 */
	public void unbindBackend(MySQLMetaBean mySQLMetaBean,String reason){
		List<MySQLSession> list = backendMap.get(mySQLMetaBean.getRepBean());
		
		if(list!=null&&!list.isEmpty()){
			list.stream().forEach(f->{
				f.setMycatSession(null);
				f.close(true, reason);
			});
		}
		if(curBackend!=null&&curBackend.getMySQLMetaBean().equals(mySQLMetaBean)){
			curBackend = null;
		}
	}

	/**
	 * 获取ProxyBuffer控制权，同时设置感兴趣的事件，如SocketRead，Write，只能其一
	 *
	 * @param intestOpts
	 * @return
	 */
	public void takeOwner(int intestOpts) {
		this.curBufOwner = true;
		if (intestOpts == SelectionKey.OP_READ) {
			this.change2ReadOpts();
		} else {
			this.change2WriteOpts();
		}
		if (this.curBackend != null) {
			curBackend.setCurBufOwner(false);
			curBackend.clearReadWriteOpts();
		}
	}

	/**
	 * 放弃控制权，同时设置对端MySQLSession感兴趣的事件，如SocketRead，Write，只能其一
	 *
	 * @param intestOpts
	 */
	public void giveupOwner(int intestOpts) {
		this.curBufOwner = false;
		this.clearReadWriteOpts();
		curBackend.setCurBufOwner(true);
		if (intestOpts == SelectionKey.OP_READ) {
			curBackend.change2ReadOpts();
		}
//		else {
//			curBackend.change2WriteOpts();
//		}
	}

	/**
	 * 向前端发送数据报文,需要先确定为Write状态并确保写入位置的正确（frontBuffer.writeState)
	 *
	 * @param rawPkg
	 * @throws IOException
	 */
	public void answerFront(byte[] rawPkg) throws IOException {
		proxyBuffer.writeBytes(rawPkg);
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		writeToChannel();
	}

	public void close(boolean normal, String hint) {
		super.close(normal, hint);
		//TODO 清理前后端资源
		this.unbindAllBackend();
		//((MyCommand)this.getSQLCmd(DefaultIntercepor.INSTANCE)).clearFrontResouces(this, true);
		clearSQLCmdsFrontResouces(true);
		//this.curSQLCommand.clearFrontResouces(this, true);
	}

	@Override
	protected void doTakeReadOwner() {
		this.takeOwner(SelectionKey.OP_READ);
	}


	private String getbackendName(){
		String backendName = null;
		switch(schema.type){
			case DB_IN_ONE_SERVER:
				backendName = schema.getDefaultDN().getMysqlReplica();
				break;
			case ANNOTATION_ROUTE:
				break;
			case DB_IN_MULTI_SERVER:
				break;
			case SQL_PARSE_ROUTE:
				break;
			default:
				break;
		}
		if(backendName==null){
			throw new InvalidParameterException("the backendName must not be null");
		}
		return backendName;
	}

	/**
	 * 将后端连接放入到后端连接缓存中
	 * @param mysqlSession
	 */
	private void putbackendMap(MySQLSession mysqlSession){
		
		List<MySQLSession> list = backendMap.get(mysqlSession.getMySQLMetaBean().getRepBean());
		if (list == null){
			list = new ArrayList<>();
			backendMap.putIfAbsent(mysqlSession.getMySQLMetaBean().getRepBean(), list);
		}
		logger.debug("add backend connection in mycatSession .{}:{}",mysqlSession.getMySQLMetaBean().getIp(),mysqlSession.getMySQLMetaBean().getPort());
		list.add(mysqlSession);
	}

	/**
	 * 当前操作的后端会话连接
	 *
	 * @return
	 */
	public void getBackend(AsynTaskCallBack<MySQLSession> callback) throws IOException {
		MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
		
		final boolean runOnSlave = canRunOnSlave();
		
		MySQLRepBean repBean = getMySQLRepBean(getbackendName());
		
		/**
		 * 本次根据读写分离策略要使用的metaBean
		 */
		MySQLMetaBean targetMetaBean = repBean.getBalanceMetaBean(runOnSlave);
		
		if(targetMetaBean==null){
			String errmsg = " the metaBean is not found,please check datasource.yaml!!! [balance] and [type]  propertie or view debug log or check heartbeat task!!";
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
		
		/*
		 * 连接复用优先级
		 * 1. 当前正在使用的 backend
		 * 2. 当前session 缓存的 backend
		 */

		//1. 当前正在使用的 backend
		// 当前连接如果本次不被使用,会被自动放入 currSessionMap 中
		if (curBackend != null
				&& canUseforCurrent(curBackend,targetMetaBean,runOnSlave)){
			if (logger.isDebugEnabled()){
				logger.debug("Using cached backend connections for {}。{}:{}"
							,(runOnSlave ? "read" : "write"),
							curBackend.getMySQLMetaBean().getIp(),
							curBackend.getMySQLMetaBean().getPort());
			}
			reactorThread.syncAndExecute(curBackend,callback);
			return;
		}

		//2. 当前session 缓存的 backend
		MySQLSession mysqlSession = getCurrCachedSession(targetMetaBean, runOnSlave,true);

		if(mysqlSession!=null){
			bindBackend(mysqlSession);
			reactorThread.syncAndExecute(mysqlSession,callback);
		}else{
			//3. 从当前 actor 中获取连接
			reactorThread.getMySQLSession(this,runOnSlave,targetMetaBean,callback);
		}
	}
	
	/**
	 * 判断连接是否可以被 当前操作使用
	 * @param balanceType
	 * @param backend
	 * @param runOnSlave
	 * @return
	 */
    private boolean canUseforCurrent(MySQLSession backend,MySQLMetaBean targetMetaBean,boolean runOnSlave){
    	
    	MySQLMetaBean currMetaBean = backend.getMySQLMetaBean();
    	    	
    	if(targetMetaBean==null){
    		return false;
    	}

    	if(currMetaBean.equals(targetMetaBean)){
    		return true;
    	}else{
    		return false;
    	}
    }
    
    /**
     * 获取指定的复制组
     * @param replicaName
     * @return
     */
    private MySQLRepBean getMySQLRepBean(String replicaName){
       	MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
		MySQLRepBean repBean = conf.getMySQLRepBean(replicaName);
		if (repBean == null) {
			throw new RuntimeException("no such MySQLRepBean " + replicaName);
		}
		return repBean;
    }
    
    /**
     * 获取 MySQLMetaBean 的一个空闲连接
     * @param metaBean
     * @return
     */
    public MySQLSession getMySQLSession(MySQLMetaBean metaBean){
		List<MySQLSession> backendList = backendMap.get(metaBean.getRepBean());
		if (backendList == null || backendList.isEmpty()) {
			return null;
		}
		
		return backendList.stream().filter(f -> {
				Boolean flag = (Boolean) f.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
				return (flag == null) ? false : flag;
			})
			.findFirst()
			.orElse(null);
	}

    /**
     * 从后端连接中获取满足条件的连接
     * 1. 主从节点
     * 2. 空闲节点
     */
    public MySQLSession getCurrCachedSession(MySQLMetaBean targetMetaBean, boolean runOnSlave,boolean isOnlyIdle) {
		MySQLSession result = null;
		
		MySQLRepBean repBean = targetMetaBean.getRepBean();

		List<MySQLSession> backendList = backendMap.get(repBean);
		if (backendList == null || backendList.isEmpty()) {
			return null;
		}
		//TODO 暂时不考虑分片情况下,分布式事务的问题。
		result = backendList.stream().filter(f -> {

				if(!targetMetaBean.equals(f.getMySQLMetaBean())){
					return false;
				}
				
				if (isOnlyIdle) {
					Boolean flag = (Boolean) f.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
					return (flag == null) ? false : flag;
				}
				return true;
			})
			.findFirst()
			.orElse(null);

		if (result != null) {
			//取消绑定，不主动绑定.当前方法可能被 reactor 其他session 调用。在这里只取消绑定
			if(curBackend!=null&&
					curBackend.equals(result)){
				curBackend = null;
			}
			backendList.remove(result);
			if (logger.isDebugEnabled()){
				logger.debug("Using SessionMap backend connections for {}.{}:{}",
							(runOnSlave ? "read" : "write"),
							result.getMySQLMetaBean().getIp(),
							result.getMySQLMetaBean().getPort());
			}
			return result;
		}
		return result;
    }

	/*
	 * 判断后端连接 是否可以走从节点
	 * 1. TODO 通过注解走读写分离
	 * 2. 非事务情况下，走读写分离
	 * 3. TODO 只读事务情况下，走读写分离
	 * @return
	 */
	private boolean canRunOnSlave(){

		if((NewSQLContext.ANNOTATION_BALANCE==sqlContext.getAnnotationType()
				||(NewSQLContext.ANNOTATION_DB_TYPE==sqlContext.getAnnotationType()
				   &&1==sqlContext.getAnnotationValue(NewSQLContext.ANNOTATION_DB_TYPE)))
			||(AutoCommit.ON==autoCommit  //非事务场景下，走从节点
			)){  // 事务场景下, 如果配置了事务内的查询也走读写分离

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
	
	/*
	 * 获取inceptor对应的SQLCommand 
	 *
	 */
	public MyCommand getSQLCmd(Interceptor inteceptor) {
		
		return SQLCommands.get(inteceptor);
	}
	/*
	 * 放置inceptor对应的SQLCommand 
	 *
	 */
	public void putSQLCmd(Interceptor inteceptor, MyCommand command) {
		SQLCommands.put(inteceptor, command);
	}
	
	public void clearSQLCmdsFrontResouces(boolean sessionCLosed) {
		interceptorList.forEach(interceptor -> {
			MyCommand command = getSQLCmd(interceptor);
			if(null != command) {
				command.clearFrontResouces(this, sessionCLosed);
			}
		});
	}
	
	public void clearSQLCmdsBackendResouces(MySQLSession  mysqlSession, boolean sessionCLosed) {
		interceptorList.forEach(interceptor -> {
			MyCommand command = getSQLCmd(interceptor);
			if(null != command) {
				command.clearBackendResouces(mysqlSession, sessionCLosed);
			}
		});
			
	}
	
	
}
