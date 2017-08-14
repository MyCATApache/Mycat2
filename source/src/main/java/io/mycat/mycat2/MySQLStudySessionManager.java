package io.mycat.mycat2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.net.DefaultMySQLStudySessionHandler;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.Session;
import io.mycat.proxy.SessionManager;

/**
 * 用来分析MySQL报文协议的SessionManager，运行过程中打印收发到的消息报文
 * 
 * @author wuzhihui
 *
 */
public class MySQLStudySessionManager implements SessionManager<MySQLSession> {
	protected static Logger logger = LoggerFactory.getLogger(MySQLStudySessionManager.class);
	private ArrayList<MySQLSession> allSessions = new ArrayList<MySQLSession>();

	@Override
	public MySQLSession createSession(Object keyAttachment, BufferPool bufPool, Selector nioSelector,
			SocketChannel frontChannel, boolean isAcceptCon) throws IOException {

		logger.info("MySQL client connected  ." + frontChannel);

		MySQLSession session = new MySQLSession(bufPool, nioSelector, frontChannel);
		session.setCurNIOHandler(DefaultMySQLStudySessionHandler.INSTANCE);
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

		session.setSessionManager(this);
		allSessions.add(session);
		logger.info("Connecting to server " + serverIP + ":" + serverPort);
		return session;
	}

	@Override
	public Collection<MySQLSession> getAllSessions() {
		return this.allSessions;
	}

	public void removeSession(Session session) {
		this.allSessions.remove(session);

	}
}
