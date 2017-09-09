package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.InvalidParameterException;
import java.util.*;

import io.mycat.mycat2.beans.*;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.ProxyReactorThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.cmds.strategy.AnnotateRouteCmdStrategy;
import io.mycat.mycat2.cmds.strategy.DBINMultiServerCmdStrategy;
import io.mycat.mycat2.cmds.strategy.DBInOneServerCmdStrategy;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendSynchemaTask;
import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyRuntime;
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

	public MyCommand curSQLCommand;

	public NewSQLContext sqlContext = new NewSQLContext();

	/**
	 * Mycat Schema
	 */
	public SchemaBean schema;

	private Map<String,List<MySQLSession>> backendMap = new HashMap<>();

	private static List<Byte> masterSqlList = new ArrayList<>();

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
	}

	/**
	 * 获取sql 类型
	 * @return
	 */
	public MyCommand getMyCommand(){
		switch(schema.type){
		case DBInOneServer:
			return DBInOneServerCmdStrategy.INSTANCE.getMyCommand(this);
		case DBINMultiServer:
			return DBINMultiServerCmdStrategy.INSTANCE.getMyCommand(this);
		case AnnotateRoute:
			return AnnotateRouteCmdStrategy.INSTANCE.getMyCommand(this);
		case SQLParseRoute:
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

	/**
	 * 绑定后端MySQL会话
	 *
	 * @param backend
	 */
	public void bindBackend(MySQLSession backend) {
		this.curBackend = backend;
		putbackendMap(backend);
		backend.setMycatSession(this);
		backend.useSharedBuffer(this.proxyBuffer);
		backend.setCurNIOHandler(this.getCurNIOHandler());
	}

	/**
	 * 将所有后端连接归还到ds中
	 */
	public void unbindAllBackend() {
		final ProxyReactorThread reactor = (ProxyReactorThread) Thread.currentThread();
		backendMap.forEach((key, value) -> {
			if (value != null) {
				value.forEach(mySQLSession -> {
					mySQLSession.unbindMycatSession();
					reactor.addMySQLSession(mySQLSession.getMySQLMetaBean(), mySQLSession);
				});
			}
		});
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
		} else {
			curBackend.change2WriteOpts();
		}
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
		this.curSQLCommand.clearResouces(true);
	}

	public MySQLMetaBean getDatasource(boolean runOnSlave) {
		SchemaBean schemaBean = this.schema;
		MycatConfig mycatConf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
		if (schemaBean == null) {
			schemaBean = mycatConf.getDefaultMycatSchema();
		}
		DNBean dnBean = schemaBean.getDefaultDN();
		String replica = dnBean.getMysqlReplica();
		MySQLRepBean repSet = mycatConf.getMySQLReplicat(replica);
		MySQLMetaBean datas = runOnSlave ? repSet.getLBReadMetaBean() : repSet.getCurWriteMetaBean();
		return datas;
	}

	@Override
	protected void doTakeReadOwner() {
		this.takeOwner(SelectionKey.OP_READ);
	}


	private String getbackendName(){
		String backendName = null;
		switch(schema.type){
		case DBInOneServer:
			backendName = schema.getDefaultDN().getMysqlReplica();
			break;
		case AnnotateRoute:
			break;
		case DBINMultiServer:
			break;
		case SQLParseRoute:
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
		String backendName = getbackendName();
		mysqlSession.setCurrBackendCachedName(backendName);
		List<MySQLSession> list = backendMap.get(backendName);
		if(list==null){
			list = new ArrayList<>();
			backendMap.putIfAbsent(backendName, list);
		}
		list.add(mysqlSession);
	}

	/**
	 * 当前操作的后端会话连接
	 *
	 * @return
	 */
	public void getBackend(AsynTaskCallBack<MySQLSession> callback) throws IOException {
		final boolean runOnSlave = canRunOnSlave();

		String backendName = getbackendName();
		/*
		 * 连接复用优先级
		 * 1. 当前正在使用的 backend
		 * 2. 当前session 缓存的 backend
		 * 3. reactor thread中空闲的backend
		 * 4. 连接池中的 backend
		 * 5. 是否可以新建连接
		 */

		//1. 当前正在使用的 backend
		if (curBackend != null
                && backendName.equals(curBackend.getCurrBackendCachedName())
				&& curBackend.isDefaultChannelRead() == runOnSlave){
			if (logger.isDebugEnabled()){
				logger.debug("Using cached backend connections for " + (runOnSlave ? "read" : "write"));
			}
			callback.finished(curBackend,null,true,null);
			return;
		}

		//2. 当前session 缓存的 backend
		MySQLSession mysqlSession = getFirstSession(this, backendName, false, runOnSlave, false);

		//3. 从reactor的其他MycatSession中获取空闲连接
		final ProxyReactorThread reactorThread = (ProxyReactorThread) Thread.currentThread();
		if (mysqlSession == null) {
			LinkedList<MycatSession> mycatSessions = reactorThread.getAllSessions();
            mysqlSession = mycatSessions.stream()
                    .map(mycatSession -> getFirstSession(mycatSession, backendName, true, runOnSlave, true))
                    .filter(session -> session != null).findFirst().orElse(null);
            if (mysqlSession != null) {
                mysqlSession.unbindMycatSession();
            }
		}

		if (mysqlSession == null){
			if(logger.isDebugEnabled()){
				logger.debug("create new connection for "+(runOnSlave?"read":"write"));
			}

			final MySQLMetaBean mySQLMetaBean = this.getDatasource(runOnSlave);
			//4. 从ds中获取已经建立的连接
            mysqlSession = reactorThread.getExistsSession(mySQLMetaBean);

            // 5. 新建连接
            if (mysqlSession == null) {
				reactorThread.createSession(mySQLMetaBean, schema, (optSession, Sender, exeSucces, retVal) -> {
					MySQLSession mySQLSession = (MySQLSession) optSession;
					//设置当前连接 读写分离属性
					optSession.setDefaultChannelRead(runOnSlave);
					//恢复默认的Handler
					this.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
					mySQLSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
					if (exeSucces) {
						this.bindBackend(mySQLSession);
						syncSessionStateToBackend(mySQLSession,callback);
					} else {
						ErrorPacket errPkg = (ErrorPacket) retVal;
						this.responseOKOrError(errPkg);
					}
				});
                return;
            }
		}

		curBackend = mysqlSession;
        this.bindBackend(curBackend);
		if (logger.isDebugEnabled()) {
			logger.debug("Using cached map backend connections for "+ (runOnSlave ? "read" : "write"));
		}
		callback.finished(curBackend,null,true,null);
	}

    /**
     * 从后端连接中获取满足条件的连接
     * 1. 主从节点
     * 2. 空闲节点
     */
    private MySQLSession getFirstSession(MycatSession mycatSession, String backendName, boolean checkCurBackend, boolean runOnSlave, boolean isIdle) {
        if (checkCurBackend) {
            MySQLSession curBack = mycatSession.curBackend;
            if (curBack != null
                    && backendName.equals(curBack.getCurrBackendCachedName())
                    && curBack.isDefaultChannelRead() == runOnSlave) {
                //判断连接是否空闲
                Boolean idle = (Boolean) curBack.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
                if (idle != null && idle == true) {
                    return curBack;
                }
            }
        }

        List<MySQLSession> backendList = mycatSession.backendMap.get(backendName);
        if (backendList == null)
            return null;

        return backendList.stream().filter(f -> {
                boolean idleFlag = true;
                if (isIdle) {
                    Boolean flag = (Boolean) f.getSessionAttrMap().get(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey());
                    idleFlag = (flag == null) ? false : flag;
                }
                return f.isDefaultChannelRead() == runOnSlave && idleFlag;
            }).findFirst().orElse(null);
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

	/**
	 * 同步后端连接状态
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	public void syncSessionStateToBackend(MySQLSession mysqlSession,AsynTaskCallBack<MySQLSession> callback) throws IOException {
		MycatSession mycatSession = mysqlSession.getMycatSession();
		BackendSynchronzationTask backendSynchronzationTask = new BackendSynchronzationTask(mysqlSession);
		backendSynchronzationTask.setCallback((optSession, sender, exeSucces, rv) -> {
			//恢复默认的Handler
			mycatSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
			optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
			if (exeSucces) {
				syncSchemaToBackend(optSession,callback);
			} else {
				ErrorPacket errPkg = (ErrorPacket) rv;
				mycatSession.close(true, errPkg.message);
			}
		});
		mycatSession.setCurNIOHandler(backendSynchronzationTask);
	}

	/**
	 * 同步 schema 到后端
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	public void syncSchemaToBackend(MySQLSession mysqlSession,AsynTaskCallBack<MySQLSession> callback)  throws IOException{
		if(mysqlSession.getMycatSession().schema!=null
				&&!mysqlSession.getMycatSession().schema.getDefaultDN().getDatabase().equals(mysqlSession.getDatabase())){
			MycatSession mycatSession = mysqlSession.getMycatSession();
			BackendSynchemaTask backendSynchemaTask = new BackendSynchemaTask(mysqlSession);
			backendSynchemaTask.setCallback((optSession, sender, exeSucces, rv) -> {
				//恢复默认的Handler
				mycatSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
				optSession.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
				if (exeSucces) {
					if(callback!=null){
						callback.finished(optSession, sender, exeSucces, rv);
					}
				} else {
					ErrorPacket errPkg = (ErrorPacket) rv;
					mycatSession.close(true, errPkg.message);
				}
			});
			mycatSession.setCurNIOHandler(backendSynchemaTask);
		}else{
			if(callback!=null){
				callback.finished(mysqlSession, null, true, null);
			}
		}
	}

}
