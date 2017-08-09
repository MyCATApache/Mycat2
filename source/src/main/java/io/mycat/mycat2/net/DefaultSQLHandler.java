package io.mycat.mycat2.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLReplicatSet;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.DNBean;
import io.mycat.mycat2.beans.MySQLDataSource;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.DefaultDirectProxyHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;

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
	public void onFrontRead(final MySQLSession session) throws IOException {
		boolean readed = session.readSocket(true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false
				|| session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf, false) == false) {
			return;
		}
		if (backendBuffer.readState.hasRemain()) {
			logger.warn("front read half package ");
		}
		if (session.backendChannel == null) {
            //todo ，从连接池中获取连接，获取不到后创建新连接，
			final DNBean dnBean = session.schema.getDefaultDN();
	        final String replica = dnBean.getMysqlReplica();
	        MycatConfig mycatConf=(MycatConfig)ProxyRuntime.INSTANCE.getProxyConfig();
	        final MySQLReplicatSet repSet = mycatConf.getMySQLReplicatSet(replica);
	        final MySQLDataSource datas = repSet.getCurWriteDH();
	        
			logger.info("hang cur sql for  backend connection ready ");
			String serverIP = "localhost";
			int serverPort = 3306;
			InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
			session.backendChannel = SocketChannel.open();
			session.backendChannel.configureBlocking(false);
			session.backendChannel.connect(serverAddress);
			SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT,
					session);
			session.backendKey = selectKey;
			logger.info("Connecting to server " + serverIP + ":" + serverPort);

			BackendConCreateTask authProcessor = new BackendConCreateTask(session,null);
			authProcessor.setCallback((optSession, Sender, exeSucces, retVal) -> {
				if (exeSucces) {
					optSession.setCurProxyHandler(DefaultSQLHandler.INSTANCE);
					// 透传前端发送的命令给Server
					session.backendBuffer.flip();
					session.writeToChannel(session.backendBuffer, session.backendChannel);
				} else {
					ErrorPacket errPkg = (ErrorPacket) retVal;
					optSession.responseOKOrError(errPkg, true);

				}
			});
			session.setCurProxyHandler(authProcessor);
			return;

		} else {
			//直接透传报文
			backendBuffer.flip();
			session.writeToChannel(backendBuffer, session.backendChannel);
			return;
		}

	}
	
	public void onBackendRead(MySQLSession session) throws IOException {
		boolean readed = session.readSocket(false);
		if (readed == false
				|| session.resolveMySQLPackage(session.frontBuffer, session.curBackendMSQLPackgInf, false) == false) {
			return;
		}
		//直接透传
		session.frontBuffer.flip();
		session.modifySelectKey();

	}


}
