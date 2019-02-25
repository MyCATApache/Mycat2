package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.net.MySQLClientAuthHandler;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;

/**
 * Mycat 2.0 Session Manager
 * 
 * @author wuzhihui
 *
 */
public class MycatSessionManager implements SessionManager<MycatSession> {
	protected static Logger logger = LoggerFactory.getLogger(MycatSessionManager.class);
	private ArrayList<MycatSession> allSessions = new ArrayList<MycatSession>();

	@Override
	public MycatSession createSession(Object keyAttachment, BufferPool bufPool, Selector nioSelector,
			SocketChannel frontChannel) throws IOException {
		if (logger.isInfoEnabled()) {
			logger.info("MySQL client connected  ." + frontChannel);
		}
		MycatSession session = new MycatSession(bufPool, nioSelector, frontChannel);
		// 第一个IO处理器为Client Authorware
		session.setCurNIOHandler(MySQLClientAuthHandler.INSTANCE);
		// 默认为透传命令模式
		// session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
		// 向MySQL Client发送认证报文
		session.sendAuthPackge();
		session.setSessionManager(this);
		allSessions.add(session);
		return session;
	}

	@Override
	public Collection<MycatSession> getAllSessions() {
		return this.allSessions;
	}

	public void removeSession(MycatSession session) {
		this.allSessions.remove(session);

	}

	@Override
	public int curSessionCount() {

		return allSessions.size();
	}

	@Override
	public NIOHandler<MycatSession> getDefaultSessionHandler() {
		return MainMycatNIOHandler.INSTANCE;
	}

}
