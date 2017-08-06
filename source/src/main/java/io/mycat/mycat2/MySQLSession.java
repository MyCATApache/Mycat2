package io.mycat.mycat2;

import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.proxy.BufferPool;
import io.mycat.proxy.UserSession;

public class MySQLSession extends UserSession {

	/**
	 * 当前处理中的SQL报文的信息
	 */
	public MySQLPackageInf curMSQLPackgInf = new MySQLPackageInf();
    public SQLProcessor curSQLProcessor;
	public MySQLSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) {
		super(bufPool, nioSelector, frontChannel);

	}

	public void setCurrentSQLProcessor(SQLProcessor sqlCmd) {
		curSQLProcessor=sqlCmd;
	}

	public SQLProcessor getCurSQLProcessor() {
		return curSQLProcessor;
	}

	public void pushSQLCmd(SQLProcessor sqlCmd) {

	}

	public void popSQLCmd() {

	}

}
