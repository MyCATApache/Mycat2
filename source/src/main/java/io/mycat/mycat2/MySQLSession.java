package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.CapabilityFlags;
import io.mycat.proxy.buffer.BufferPool;

/**
 * 后端MySQL连接
 *
 * @author wuzhihui
 *
 */
public class MySQLSession extends AbstractMySQLSession {

	private String database;
	/**
	 * 当前所从属的mycat sesssion
	 */
	private MycatSession mycatSession;

	// 记录当前后端连接所属的MetaBean，用于后端连接归还使用
	private MySQLMetaBean mysqlMetaBean;

	public MySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel, SelectionKey.OP_CONNECT);
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
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
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
