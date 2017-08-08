package io.mycat.mycat2.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.tasks.BackendAuthProcessor;
import io.mycat.proxy.DefaultDirectProxyHandler;
import io.mycat.proxy.ProxyBuffer;

/**
 * 负责处理通用的SQL命令，默认情况下透传
 * 
 * @author wuzhihui
 *
 */
public class DefaultSQLHandler extends DefaultDirectProxyHandler<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(DefaultSQLHandler.class);
	public static DefaultSQLHandler INSTANCE = new DefaultSQLHandler();
	// private ArrayList<Runnable> pendingJob;

	@Override
	public void onFrontRead(MySQLSession session) throws IOException {
		boolean readed = session.readSocket(true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false || session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf) == false) {
			return;
		}
		if (backendBuffer.readState.hasRemain()) {
			logger.warn("front read half package ");
		}
		if(session.backendChannel==null)
		{
			
			String serverIP = "localhost";
			int serverPort = 3306;
			InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
			session.backendChannel = SocketChannel.open();
			session.backendChannel.configureBlocking(false);
			session.backendChannel.connect(serverAddress);
			SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT, session);
			session.backendKey = selectKey;
			logger.info("Connecting to server " + serverIP + ":" + serverPort);
			
			BackendAuthProcessor authProcessor=new BackendAuthProcessor(session);
			session.setCurProxyHandler(authProcessor);
			return;
			
		}else
		{
			session.writeToChannel(backendBuffer, session.backendChannel);
			return;
		}
		

	}



}
