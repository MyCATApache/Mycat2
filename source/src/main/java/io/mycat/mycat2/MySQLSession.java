package io.mycat.mycat2;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendSynchemaTask;
import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.proxy.buffer.BufferPool;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.commons.lang.StringUtils;

/**
 * 后端MySQL连接
 *
 * @author wuzhihui
 */
public class MySQLSession extends AbstractMySQLSession {

	private String database;
	/**
	 * 当前所从属的mycat sesssion
	 */
	private MycatSession mycatSession;

	// 记录当前后端连接所属的MetaBean，用于后端连接归还使用
	private MySQLMetaBean mysqlMetaBean;

	/**
	 * 标识当前连接的闲置状态标识 ，true，闲置，false，未闲置,即在使用中
	 */
	boolean idleFlag = true;

	public void setIdle(boolean idleFlag) {
		this.idleFlag = idleFlag;
	}

	public boolean isIdle() {
		return this.idleFlag;
	}

	public MySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel, SelectionKey.OP_CONNECT);
	}

	/**
	 * 是否与前端的状态保持同步，包括如下几点： Schema(Database名字） 事务隔离级别 字符集 事务提交方式
	 * 
	 * @param targetDataBase目标数据库，即当前连接是否在此数据库上（use
	 *            dbxxx），null表示不考虑数据库是否匹配
	 * @return
	 */
	public boolean synchronizedState(String targetDataBase) {
		if (!getMySQLMetaBean().isSlaveNode()) {
			// 隔离级别
			if (mycatSession.isolation != isolation) {
				return false;
			}
			// 提交方式同步
			if (mycatSession.autoCommit != autoCommit) {
				return false;
			}
		}
		// 字符集同步
		if (mycatSession.charSet.charsetIndex != charSet.charsetIndex) {
			return false;
		} else if (targetDataBase != null && !targetDataBase.equals(this.database)) {
			return false;
		}
		return true;
	}

	/**
	 * 同步后端连接状态,并在完成后回调发起者
	 *
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	public void syncAndCallback(AsynTaskCallBack<MySQLSession> callback) throws IOException {
		MycatSession mycatSession = getMycatSession();
		BackendSynchronzationTask backendSynchronzationTask = new BackendSynchronzationTask(mycatSession, this);
		backendSynchronzationTask.setCallback((optSession, sender, exeSucces, rv) -> {
			// 恢复默认的Handler
			mycatSession.setCurNIOHandler(MainMycatNIOHandler.INSTANCE);
			optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
			if (exeSucces) {
				if(!optSession.getMycatSession().getTargetDataNode().getDatabase().equals(optSession.getDatabase()))
				{
					this.syncSchemaAndCallback(callback);
				}
				
			} else {
				// ErrorPacket errPkg = (ErrorPacket) rv;
				// mycatSession.close(true, errPkg.message);
				callback.finished(optSession, sender, exeSucces, rv);
			}
		});
		backendSynchronzationTask.syncState(mycatSession, this);
		// mycatSession.setCurNIOHandler(backendSynchronzationTask);
	}

	/**
	 * （只）同步MySQLSession的Database到MycatSession的targetDataNode所对应的Database
	 *
	 * @param mysqlSession
	 * @param callback
	 * @throws IOException
	 */
	private void syncSchemaAndCallback(AsynTaskCallBack<MySQLSession> callback) throws IOException {
		BackendSynchemaTask backendSynchemaTask = new BackendSynchemaTask(this, (optSession, sender, exeSucces, rv) -> {
			// 恢复默认的Handler
			mycatSession.setCurNIOHandler(MainMycatNIOHandler.INSTANCE);
			optSession.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
			callback.finished(optSession, sender, exeSucces, rv);
		});
		mycatSession.setCurNIOHandler(backendSynchemaTask);
	}

	public MycatSession getMycatSession() {
		return mycatSession;
	}

	@Override
	public void close(boolean normal, String hint) {
		super.close(normal, hint);
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public void setMycatSession(MycatSession mycatSession) {
		this.mycatSession = mycatSession;
	}

	@Override
	protected void doTakeReadOwner() {
		this.getMycatSession().takeOwner(SelectionKey.OP_READ);
	}

	public MySQLMetaBean getMySQLMetaBean() {
		return mysqlMetaBean;
	}

	public void setMySQLMetaBean(MySQLMetaBean metaBean) {
		this.mysqlMetaBean = metaBean;
	}

	private static int initClientFlags() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		// flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		boolean usingCompress = false;
		if (usingCompress) {
			flag |= Capabilities.CLIENT_COMPRESS;
		}
		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= Capabilities.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		flag |= Capabilities.CLIENT_PLUGIN_AUTH;
		// // client extension
		// flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
		// flag |= Capabilities.CLIENT_MULTI_RESULTS;
		return flag;
	}

	private static CapabilityFlags capabilityFlags = new CapabilityFlags(initClientFlags());

	public static CapabilityFlags getClientCapabilityFlags() {
		return capabilityFlags;
	}

	@Override
	public String toString() {
		return "MySQLSession [sessionId = " + getSessionId() + " , database=" + database + ", ip="
				+ mysqlMetaBean.getDsMetaBean().getIp() + ",port=" + mysqlMetaBean.getDsMetaBean().getPort()
				+ ",hashCode=" + this.hashCode() + "]";
	}
}
