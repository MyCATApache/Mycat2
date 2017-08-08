package io.mycat.mycat2.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.SessionManager;

/**
 * Mycat 2.0 Session Manager
 * 
 * @author wuzhihui
 *
 */
public class MycatSessionManager implements SessionManager<MySQLSession> {
	protected static Logger logger = LoggerFactory.getLogger(MycatSessionManager.class);

	@Override
	public MySQLSession createSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel)
			throws IOException {

		logger.info("MySQL client connected  ." + frontChannel);

		MySQLSession session = new MySQLSession(bufPool, nioSelector, frontChannel);
		session.bufPool = bufPool;
		session.nioSelector = nioSelector;
		session.frontChannel = frontChannel;
		InetSocketAddress clientAddr = (InetSocketAddress) frontChannel.getRemoteAddress();
		session.frontAddr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		SelectionKey socketKey = frontChannel.register(nioSelector, SelectionKey.OP_READ, session);
		session.frontKey = socketKey;
		// 第一个IO处理器为Client Authorware
		session.setCurProxyHandler(MySQLClientAuthHandler.INSTANCE);
		//向MySQL Client发送认证报文
		session.sendAuthPackge();
		return session;
	}

}
