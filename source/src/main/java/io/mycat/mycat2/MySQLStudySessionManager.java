package io.mycat.mycat2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.net.MySQLProcalDebugHandler;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.SessionManager;

/**
 * 用来分析MySQL报文协议的SessionManager，运行过程中打印收发到的消息报文
 * 
 * @author wuzhihui
 *
 */
public class MySQLStudySessionManager implements SessionManager<MySQLSession> {
	protected static Logger logger = LoggerFactory.getLogger(MySQLStudySessionManager.class);

	@Override
	public MySQLSession createSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel)
			throws IOException {

		logger.info("MySQL client connected  ." + frontChannel);

		MySQLSession session = new MySQLSession(bufPool, nioSelector, frontChannel);
		session.setCurProxyHandler(MySQLProcalDebugHandler.INSTANCE);

		// // todo ,from config
		// // 尝试连接Server 端口
		String serverIP = "localhost";
		int serverPort = 3306;
		InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
		session.backendChannel = SocketChannel.open();
		session.backendChannel.configureBlocking(false);
		session.backendChannel.connect(serverAddress);
		SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT, session);
		session.backendKey = selectKey;
		logger.info("Connecting to server " + serverIP + ":" + serverPort);
		return session;
	}

}
