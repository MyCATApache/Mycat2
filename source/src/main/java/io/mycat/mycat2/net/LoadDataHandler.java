package io.mycat.mycat2.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.LoadDataCmd;
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
public class LoadDataHandler implements FrontIOHandler<MySQLSession>, BackendIOHandler<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(LoadDataHandler.class);
	public static LoadDataHandler INSTANCE = new LoadDataHandler();
	public static DirectPassthrouhCmd defaultSQLCmd = new DirectPassthrouhCmd();

	/**
	 * 进行load data命令的透传
	 */
	private static LoadDataCmd LOADDATA = new LoadDataCmd();

	@Override
	public void onFrontRead(final MySQLSession session) throws IOException {
		boolean readed = session.readSocket(true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false) {
			return;
		}
		if (session.curFrontMSQLPackgInf.endPos < backendBuffer.getReadOptState().optLimit) {
			logger.warn("front contains multi package ");
		}
		// 交给SQLComand去处理
		if (session.curSQLCommand.procssSQL(session, false)) {
			session.curSQLCommand.clearResouces(false);
			session.curSQLCommand = defaultSQLCmd;
		}

	}

	public void onBackendRead(MySQLSession session) throws IOException {
		boolean readed = session.readSocket(false);

		ProxyBuffer backendBuffer = session.frontBuffer;

		if (readed == false) {
			return;
		}

		if (session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf, false) == false) {
			// 没有读到完整报文
			return;
		}

		// 交给SQLComand去处理
		if (session.curSQLCommand.procssSQL(session, true)) {
			session.curSQLCommand.clearResouces(false);
		}

		// 如果当前为load data的操作命令，在收到服务器的响应后，则进行从前向后传输开始
		if (session.curFrontMSQLPackgInf.pkgType == (byte) 0xfb) {
			// 设置lodata的透传执行
			session.curSQLCommand = LOADDATA;
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
