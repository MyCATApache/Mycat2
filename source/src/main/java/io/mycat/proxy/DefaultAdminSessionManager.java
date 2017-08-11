package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认的管理会话的SessionManager
 * @author wuzhihui
 *
 */
public class DefaultAdminSessionManager implements SessionManager<Session>{
	protected static Logger logger = LoggerFactory.getLogger(DefaultAdminSessionManager.class);
	@Override
	public Session createSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) throws IOException {
		
		Session session = new Session(bufPool, nioSelector, frontChannel);
		session.bufPool = bufPool;
		session.nioSelector = nioSelector;
		session.frontChannel = frontChannel;
		InetSocketAddress clientAddr = (InetSocketAddress) frontChannel.getRemoteAddress();
		session.frontAddr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		SelectionKey socketKey = frontChannel.register(nioSelector, SelectionKey.OP_READ, session);
		session.frontKey = socketKey;
		//session.setCurProxyHandler(proxyHandler);
		//TODO setproxy
		logger.info("front connected to admin port  ." + frontChannel+ "create session "+session);
		
		return session;
	}

	

}
