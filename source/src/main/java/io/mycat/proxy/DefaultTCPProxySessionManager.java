package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认的SessionManager，创建TCP Proxy Session
 * @author wuzhihui
 *
 */
public class DefaultTCPProxySessionManager implements SessionManager<UserProxySession>{
	protected static Logger logger = LoggerFactory.getLogger(DefaultTCPProxySessionManager.class);
	@Override
	public UserProxySession createSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel,boolean isAcceptedCon) throws IOException {
		
		UserProxySession session = new UserProxySession(bufPool, nioSelector, frontChannel);
		
		// todo ,from config
		// 尝试连接Server 端口
		String serverIP = "localhost";
		int serverPort = 3306;
		InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
		session.backendChannel = SocketChannel.open();
		session.backendChannel.configureBlocking(false);
		session.backendChannel.connect(serverAddress);
		session.curProxyHandler=new DefaultDirectProxyHandler<UserProxySession>();
		SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT, session);
		session.backendKey = selectKey;
		logger.info("Connecting to backend server " + serverIP + ":" + serverPort);
		return session;
	}

	

}
