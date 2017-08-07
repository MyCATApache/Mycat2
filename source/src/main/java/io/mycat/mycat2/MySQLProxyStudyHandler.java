package io.mycat.mycat2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.cmd.ClientAuthProcessor;
import io.mycat.mycat2.cmd.DirectPassSQLProcessor;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.Versions;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.DefaultDirectProxyHandler;
import io.mycat.util.RandomUtil;

/**
 * 代理MySQL的ProxyHandler
 * 
 * @author wuzhihui
 *
 */
public class MySQLProxyStudyHandler extends DefaultDirectProxyHandler<MySQLSession> {

	

	public void onFrontConnected(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel)
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
		session.setCurrentSQLProcessor(DirectPassSQLProcessor.INSTANCE);
	
//		// todo ,from config
//		// 尝试连接Server 端口
		String serverIP = "localhost";
		int serverPort = 3306;
		InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
		session.backendChannel = SocketChannel.open();
		session.backendChannel.configureBlocking(false);
		session.backendChannel.connect(serverAddress);
		SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT, session);
		session.backendKey = selectKey;
		logger.info("Connecting to server " + serverIP + ":" + serverPort);

	}

	@Override
	public void onFrontReaded(MySQLSession userSession) throws IOException {
		
		userSession.getCurSQLProcessor().handFrontPackage(userSession);
		
	}

	@Override
	public void onBackendReaded(MySQLSession userSession) throws IOException {

		userSession.getCurSQLProcessor().handBackendPackage(userSession);

	}

}
