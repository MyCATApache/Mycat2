package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.conf.DNBean;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.cmds.LoadDataState;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.mysql.MysqlNativePasswordPluginUtil;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.NewHandshakePacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ParseUtil;

/**
 * 前端连接会话
 *
 * @author wuzhihui
 */
public class MycatSession extends AbstractMySQLSession {

	private static Logger logger = LoggerFactory.getLogger(MycatSession.class);
	private static List<Byte> masterSqlList = new ArrayList<>();

	static {
		masterSqlList.add(BufferSQLContext.INSERT_SQL);
		masterSqlList.add(BufferSQLContext.UPDATE_SQL);
		masterSqlList.add(BufferSQLContext.DELETE_SQL);
		masterSqlList.add(BufferSQLContext.REPLACE_SQL);
		masterSqlList.add(BufferSQLContext.SELECT_INTO_SQL);
		masterSqlList.add(BufferSQLContext.SELECT_FOR_UPDATE_SQL);
		masterSqlList.add(BufferSQLContext.CREATE_SQL);
		masterSqlList.add(BufferSQLContext.DROP_SQL);
		// TODO select lock in share mode 。 也需要走主节点 需要完善sql 解析器。
		masterSqlList.add(BufferSQLContext.LOAD_SQL);
		masterSqlList.add(BufferSQLContext.CALL_SQL);
		masterSqlList.add(BufferSQLContext.TRUNCATE_SQL);

		masterSqlList.add(BufferSQLContext.BEGIN_SQL);
		masterSqlList.add(BufferSQLContext.START_SQL); // TODO 需要完善sql 解析器。 将
														// start transaction
		// 分离出来。
		masterSqlList.add(BufferSQLContext.SET_AUTOCOMMIT_SQL);
	}

	private ArrayList<MySQLSession> backends = new ArrayList<>(2);
	private int curBackendIndex = -1;
	// 当前SQL期待在哪个目标DataNode上执行
	private DNBean targetDataNode;
	// 所有处理cmd中,用来向前段写数据,或者后端写数据的cmd的
	private MySQLCommand curSQLCommand;
	public BufferSQLContext sqlContext = new BufferSQLContext();
	// 客户端连接的Mycat逻辑库
	private SchemaBean mycatSchema;
	public BufferSQLParser parser = new BufferSQLParser();
	private byte sqltype;

	public LoadDataState loadDataStateMachine = LoadDataState.NOT_LOAD_DATA;

	public byte getSqltype() {
		return sqltype;
	}

	public void setSqltype(byte sqltype) {
		this.sqltype = sqltype;
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
		flag |= Capabilities.CLIENT_PLUGIN_AUTH;
		flag |= Capabilities.CLIENT_CONNECT_ATTRS;
		return flag;
	}

	/**
	 * 给客户端（front）发送认证报文
	 *
	 * @throws IOException
	 */
	public void sendAuthPackge() throws IOException {
		byte[][] seedParts = MysqlNativePasswordPluginUtil.nextSeedBuild();
		this.seed = seedParts[2];

		// 发送握手数据包
		NewHandshakePacket hs = new NewHandshakePacket();
		hs.packetId = 0;
		hs.protocolVersion = Version.PROTOCOL_VERSION;
		hs.serverVersion = new String(Version.SERVER_VERSION);
		hs.connectionId = getSessionId();
		hs.authPluginDataPartOne = new String(seedParts[0]);
		hs.capabilities = new CapabilityFlags(getServerCapabilities());
		hs.hasPartTwo = true;
		hs.characterSet = 8;
		hs.statusFlags = 2;
		hs.authPluginDataLen = 21; // 有插件的话，总长度必是21, seed
		hs.authPluginDataPartTwo = new String(seedParts[1]);
		hs.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;
		hs.write(proxyBuffer);
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		this.writeToChannel();
	}

	/**
	 * 关闭后端连接,同时向前端返回错误信息
	 *
	 * @param normal
	 */
	public void closeAllBackendsAndResponseError(boolean normal, ErrorPacket error) {
		unbindAndCloseAllBackend(normal, error.message);
		takeBufferOwnerOnly();
		responseOKOrError(error);
	}

	/**
	 * 关闭后端连接,同时向前端返回错误信息
	 *
	 * @param normal
	 * @param errno
	 * @param error
	 * @throws IOException
	 */
	public void closeAllBackendsAndResponseError(boolean normal, int errno, String error) {
		unbindAndCloseAllBackend(normal, error);
		takeBufferOwnerOnly();
		sendErrorMsg(errno, error);
	}

	private void unbindAndCloseAllBackend(boolean normal, String hint) {
		for (MySQLSession mySQLSession : this.backends) {
			logger.debug("close mysql connection {}", mySQLSession);
			mySQLSession.close(normal, hint);
		}
		this.unbindBackends();
	}

	/**
	 * 向客户端响应 错误信息
	 *
	 * @param errno
	 * @throws IOException
	 */
	public void sendErrorMsg(int errno, String errMsg) {
		ErrorPacket errPkg = new ErrorPacket();
		errPkg.packetId = (byte) (proxyBuffer.getByte(curPacketInf.startPos + ParseUtil.mysql_packetHeader_length) + 1);
		errPkg.errno = errno;
		errPkg.message = errMsg;
		responseOKOrError(errPkg);
	}

	/**
	 * 绑定后端MySQL会话，同时作为当前的使用的后端连接(current backend) 主意：调用后，curBackendIndex会更新为
	 * backend对应的Index！
	 *
	 * @param backend
	 */
	public void bindBackend(MySQLSession backend) {
		this.curBackendIndex = putBackendMap(backend);

		logger.debug(" {} bind backConnection  for {}", this, backend);
	}

	public void unbindBackend(MySQLSession backend) {
		logger.debug(" {} unbind backConnection  for {}", this, backend);
		MySQLSession curSession = this.getCurBackend();
		if (!backends.remove(backend)) {
			throw new RuntimeException("can't find backend " + backend);
		} else {
			unbindMySQLSession(backend);
		}
		// 调整curBackendIndex
		if (curSession == backend) {
			this.curBackendIndex = -1;
		} else if (curSession != null) {
			this.curBackendIndex = this.backends.indexOf(curSession);
		}
	}

	/**
	 * 将所有后端连接归还到ds中
	 */
	public void unbindBackends() {
		for (MySQLSession mySQLSession : this.backends) {
			logger.debug("unbind mysql connection {}", mySQLSession);
			unbindMySQLSession(mySQLSession);
		}
		backends.clear();
		curBackendIndex = -1;
	}

	public MySQLSession getCurBackend() {
		return (this.curBackendIndex == -1) ? null : this.backends.get(this.curBackendIndex);
	}

	public void takeBufferOwnerOnly() {
		this.curBufOwner = true;
		MySQLSession curBackend = getCurBackend();
		if (curBackend != null) {
			curBackend.setCurBufOwner(false);
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
		MySQLSession curBackend = getCurBackend();
		if (curBackend != null) {
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
		MySQLSession curBackend = getCurBackend();
		if (curBackend != null) {
			curBackend.setCurBufOwner(true);
			if (intestOpts == SelectionKey.OP_READ) {
				curBackend.change2ReadOpts();
			} else {
				curBackend.change2WriteOpts();
			}
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
		this.unbindBackends();
	}

	@Override
	protected void doTakeReadOwner() {
		this.takeOwner(SelectionKey.OP_READ);
	}

	private String getbackendName() {
		String backendName = null;
		switch (mycatSchema.getSchemaType()) {
		case DB_IN_ONE_SERVER:
			backendName = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap().get(mycatSchema.getDefaultDataNode())
					.getReplica();
			break;
		case ANNOTATION_ROUTE:
			backendName = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap().get(mycatSchema.getDefaultDataNode())
					.getReplica();
			break;
		case DB_IN_MULTI_SERVER:
			// 在 DB_IN_MULTI_SERVER
			// 模式中,如果不指定datanode以及Replica名字取得backendName,则使用默认的
			backendName = ProxyRuntime.INSTANCE.getConfig().getMycatDataNodeMap().get(mycatSchema.getDefaultDataNode())
					.getReplica();
			break;
		default:
			break;
		}
		if (backendName == null) {
			throw new InvalidParameterException("the backendName must not be null");
		}
		return backendName;
	}

	public void responseOKOrError(byte[] pkg) throws IOException {
		this.curPacketInf.shift2DefRespPacket();
		super.responseOKOrError(pkg);
	}

	public void responseOKOrError(MySQLPacket pkg) {
		this.curPacketInf.shift2DefRespPacket();
		super.responseOKOrError(pkg);
	}

	public MySQLCommand getCurSQLCommand() {
		return curSQLCommand;
	}

	/**
	 * 将后端连接放入到后端连接缓存中
	 *
	 * @param backend
	 */
	private int putBackendMap(MySQLSession backend) {
		backend.setMycatSession(this);
		backend.useSharedBuffer(this.proxyBuffer);
		backend.setCurNIOHandler(this.getCurNIOHandler());
		backend.setIdle(false);
		this.backends.add(backend);
		int total = backends.size();
		logger.debug("add backend connection in mycatSession : {}, totals : {}  ,new bind is : {}", this, total,
				backend);
		return total - 1;
	}

	/**
	 * 获取一个后端连接（新建或者重用当前的连接）,完成后回调通知。
	 * 
	 * @return
	 */
	public void getBackendAndCallBack(AsynTaskCallBack<MySQLSession> callback) throws IOException {
		((MycatReactorThread) Thread.currentThread()).tryGetMySQLAndExecute(this, callback);

	}

	public DNBean getTargetDataNode() {
		return targetDataNode;
	}

	public void setTargetDataNode(DNBean targetDataNode) {
		logger.debug("{} set target datanode to {}", this, targetDataNode);
		this.targetDataNode = targetDataNode;
	}


	
	/**
	 * 从后端连接中获取满足条件的连接 1. 主从节点 2. 空闲节点 返回-1，表示没找到，否则对应就是backends.get(i)
	 */
	private int findMatchedMySQLSession(MySQLMetaBean targetMetaBean) {
		int findIndex = -1;
		// TODO 暂时不考虑分片情况下,分布式事务的问题。
		int total = backends.size();
		for (int i = 0; i < total; i++) {
			if (targetMetaBean.equals(backends.get(i).getMySQLMetaBean())) {
				findIndex = i;
				break;
			}
		}
		return findIndex;
	}

	/*
	 * 判断后端连接 是否可以走从节点
	 * 
	 * @return
	 */
	private boolean canRunOnSlave() {
		// 静态注解情况下 走读写分离
		if (BufferSQLContext.ANNOTATION_BALANCE == sqlContext.getAnnotationType()) {
			final long balancevalue = sqlContext.getAnnotationValue(BufferSQLContext.ANNOTATION_BALANCE);
			if (TokenHash.MASTER == balancevalue) {
				return false;
			} else if (TokenHash.SLAVE == balancevalue) {
				return true;
			} else {
				logger.error("sql balance type is invalid, run on slave [{}]", sqlContext.getRealSQL(0));
			}
			return true;
		}

		// 非事务场景下，走从节点
		if (AutoCommit.ON == autoCommit) {
			return !masterSqlList.contains(sqlContext.getSQLType());
		} else {
			return false;
		}
	}

	public SchemaBean getMycatSchema() {
		return mycatSchema;
	}

	public void setMycatSchema(SchemaBean mycatSchema) {
		logger.info("{} set Mycat schema to {}", this, mycatSchema);
		this.mycatSchema = mycatSchema;
	}

	private void unbindMySQLSession(MySQLSession mysql) {
		mysql.setMycatSession(null);
		mysql.useSharedBuffer(null);
		mysql.setCurBufOwner(true); // 设置后端连接 获取buffer 控制权
		mysql.setIdle(true);
	}

	public void switchSQLCommand(MySQLCommand newCmd) {
		logger.debug("{} switch command from {} to  {} ", this, this.curSQLCommand, newCmd);
		this.curSQLCommand = newCmd;

	}

}
