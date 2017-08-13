package io.mycat.mycat2.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.BackendIOHandler;
import io.mycat.proxy.FrontIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.UserProxySession;

/**
 * 负责处理通用的SQL命令，默认情况下透传
 * 
 * @author wuzhihui
 *
 */
public class DefaultSQLHandler implements FrontIOHandler<MySQLSession>, BackendIOHandler<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(DefaultSQLHandler.class);
	public static DefaultSQLHandler INSTANCE = new DefaultSQLHandler();
	public static DirectPassthrouhCmd defaultSQLCmd = new DirectPassthrouhCmd();

	@Override
	public void onFrontRead(final MySQLSession session) throws IOException {
		boolean readed = session.readSocket(true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false) {
			return;
		}
		if (session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf, false) == false) {
			// 没有读到完整报文
			return;
		}
		if (session.curFrontMSQLPackgInf.endPos < backendBuffer.getReadOptState().optLimit) {
			logger.warn("front contains multi package ");
		}
		if (session.backendChannel == null) {
			// todo ，从连接池中获取连接，获取不到后创建新连接，
			// final DNBean dnBean = session.schema.getDefaultDN();
			// final String replica = dnBean.getMysqlReplica();
			// MycatConfig
			// mycatConf=(MycatConfig)ProxyRuntime.INSTANCE.getProxyConfig();
			// final MySQLReplicatSet repSet =
			// mycatConf.getMySQLReplicatSet(replica);
			// final MySQLDataSource datas = repSet.getCurWriteDH();

			logger.info("hang cur sql for  backend connection ready ");
			String serverIP = "10.211.55.5";
			int serverPort = 3306;
			InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
			session.backendChannel = SocketChannel.open();
			session.backendChannel.configureBlocking(false);
			session.backendChannel.connect(serverAddress);
			SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT,
					session);
			session.backendKey = selectKey;
			logger.info("Connecting to server " + serverIP + ":" + serverPort);

			BackendConCreateTask authProcessor = new BackendConCreateTask(session, null);
			authProcessor.setCallback((optSession, Sender, exeSucces, retVal) -> {
				if (exeSucces) {
					//认证成功后开始同步会话状态至后端
					syncSessionStateToBackend(session);
				} else {
					ErrorPacket errPkg = (ErrorPacket) retVal;
					optSession.responseOKOrError(errPkg, true);

				}
			});
			session.setCurProxyHandler(authProcessor);
			return;

		} else {
			// 交给SQLComand去处理
			if (session.curSQLCommand.procssSQL(session, false)) {
				session.curSQLCommand.clearResouces(false);
			}
		}

	}

	private void syncSessionStateToBackend(MySQLSession mySQLSession) throws IOException{
		BackendSynchronzationTask backendSynchronzationTask = new BackendSynchronzationTask(mySQLSession);
		backendSynchronzationTask.setCallback((session, sender, exeSucces, rv) -> {
			if (exeSucces) {
				session.setCurProxyHandler(DefaultSQLHandler.INSTANCE);
				// 交给SQLComand去处理
				if (session.curSQLCommand.procssSQL(session, false)) {
					session.curSQLCommand.clearResouces(false);
				}
			} else {
				ErrorPacket errPkg = (ErrorPacket) rv;
				session.responseOKOrError(errPkg, true);
			}
		});
		mySQLSession.setCurProxyHandler(backendSynchronzationTask);
	}

	public void onBackendRead(MySQLSession session) throws IOException {
		boolean readed = session.readSocket(false);
		if (readed == false) {
			return;
		}
		// 交给SQLComand去处理
		if (session.curSQLCommand.procssSQL(session, true)) {
			session.curSQLCommand.clearResouces(false);
		}
	}

	@Override
	public void onBackendConnect(MySQLSession userSession, boolean success, String msg) throws IOException {
		logger.warn("not handled (expected ) onBackendConnect event " + userSession.sessionInfo());

	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onFrontSocketClosed(MySQLSession userSession, boolean normal) {
		userSession.lazyCloseSession("front closed");

	}

	/**
	 * 后端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onBackendSocketClosed(MySQLSession userSession, boolean normal) {
		userSession.lazyCloseSession("backend closed ");
	}

	/**
	 * Socket IO读写过程中出现异常后的操作，通常是要关闭Session的
	 * 
	 * @param userSession
	 * @param exception
	 */
	protected void onSocketException(UserProxySession userSession, Exception exception) {
		if (exception instanceof IOException) {
			logger.warn(
					"DefaultSQLHandler handle IO error " + userSession.sessionInfo() + " " + exception.getMessage());

		} else {
			logger.warn("DefaultSQLHandler handle IO error " + userSession.sessionInfo(), exception);
		}
		userSession.close("exception:" + exception.getMessage());
	}

	@Override
	public void onFrontWrite(MySQLSession session) throws IOException {
		session.writeToChannel(session.frontBuffer, session.frontChannel);

	}

	@Override
	public void onBackendWrite(MySQLSession session) throws IOException {
		session.writeToChannel(session.backendBuffer, session.backendChannel);

	}

}
