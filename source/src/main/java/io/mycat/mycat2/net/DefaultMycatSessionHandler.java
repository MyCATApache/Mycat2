package io.mycat.mycat2.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.MySQLDataSource;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mycat2.tasks.BackendSynchronzationTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;

/**
 * 负责MycatSession的NIO事件，驱动SQLCommand命令执行，完成SQL的处理过程
 * 
 * @author wuzhihui
 *
 */
public class DefaultMycatSessionHandler implements NIOHandler<AbstractMySQLSession> {
	public static final DefaultMycatSessionHandler INSTANCE = new DefaultMycatSessionHandler();
	private static Logger logger = LoggerFactory.getLogger(DefaultMycatSessionHandler.class);

	public void onSocketRead(final AbstractMySQLSession session) throws IOException {
		if (session instanceof MycatSession) {
			onFrontRead((MycatSession) session);
		} else {
			onBackendRead((MySQLSession) session);
		}
	}

	private void onFrontRead(final MycatSession session) throws IOException {
		boolean readed = session.readFromChannel();
		ProxyBuffer buffer = session.getProxyBuffer();
		// 在load data的情况下，SESSION_PKG_READ_FLAG会被打开，以不让进行包的完整性检查
		if (!session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_PKG_READ_FLAG.getKey())
				&& (readed == false ||
						// 没有读到完整报文
						MySQLSession.CurrPacketType.Full != session.resolveMySQLPackage(buffer, session.curMSQLPackgInf,
								false))) {
			return;
		}
		if (session.curMSQLPackgInf.endPos < buffer.writeIndex) {
			logger.warn("front contains multi package ");
		}
		if (session.getBackend() == null) {
			// todo ，从连接池中获取连接，获取不到后创建新连接，
			final MySQLDataSource ds = session.getDatasource();
			logger.info("hang cur sql for  backend connection ready ");
			BackendConCreateTask authProcessor = new BackendConCreateTask(session.bufPool, session.nioSelector, ds,
					session.schema.name);
			authProcessor.setCallback((optSession, Sender, exeSucces, retVal) -> {
				// 恢复默认的Handler
				session.setCurNIOHandler(INSTANCE);
				if (exeSucces) {
					session.bindBackend(optSession);
					if (session.curSQLCommand.procssSQL(session)) {
						session.curSQLCommand.clearResouces(false);
					}
				} else {
					ErrorPacket errPkg = (ErrorPacket) retVal;
					optSession.responseOKOrError(errPkg);

				}
			});
			session.setCurNIOHandler(authProcessor);
			return;

		} else {

			// if not synchorndiz d
			// syncSessionStateToBackend()
			// 如果是 SQL 则调用 sql parser 进行处理
			// SQLComandProcessInf sqlCmd =
			// SQLCOMMANDMAP.get(session.curFrontMSQLPackgInf.pkgType);

			// 如果当前包需要处理，则交给对应方法处理，否则直接透传
			if (session.curSQLCommand.procssSQL(session)) {
				session.curSQLCommand.clearResouces(false);
			}
		}
	}

	private void syncSessionStateToBackend(MycatSession mycatSession, MySQLSession mysqlSession) throws IOException {
		BackendSynchronzationTask backendSynchronzationTask = new BackendSynchronzationTask(mysqlSession);
		backendSynchronzationTask.setCallback((session, sender, exeSucces, rv) -> {
			if (exeSucces) {
				// 交给SQLComand去处理
				if (mycatSession.curSQLCommand.procssSQL(mycatSession)) {
					mycatSession.curSQLCommand.clearResouces(false);
				}
			} else {
				ErrorPacket errPkg = (ErrorPacket) rv;
				session.responseOKOrError(errPkg);
			}
		});
		mycatSession.setCurNIOHandler(backendSynchronzationTask);
	}

	private void onBackendRead(MySQLSession session) throws IOException {

		// 交给SQLComand去处理
		SQLCommand curCmd = session.getMycatSession().curSQLCommand;
		if (curCmd.onBackendResponse(session)) {
			curCmd.clearResouces(false);
		}

	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onSocketClosed(AbstractMySQLSession session, boolean normal) {
		if (session instanceof MycatSession) {
			logger.info("front socket closed " + session);
			session.lazyCloseSession(normal, "front closed");
		} else {
			MySQLSession mysqlSession = (MySQLSession) session;
			try {
				mysqlSession.getMycatSession().curSQLCommand.onBackendClosed(mysqlSession, normal);
			} catch (IOException e) {
				logger.warn("caught err ", e);
			}
		}
	}

	@Override
	public void onSocketWrite(AbstractMySQLSession session) throws IOException {
		session.writeToChannel();

	}

	@Override
	public void onConnect(SelectionKey curKey, AbstractMySQLSession session, boolean success, String msg)
			throws IOException {
		throw new java.lang.RuntimeException("not implemented ");
	}

	@Override
	public void onWriteFinished(AbstractMySQLSession session) throws IOException {
		// 交给SQLComand去处理
		if (session instanceof MycatSession) {
			MycatSession mycatSs = (MycatSession) session;
			if (mycatSs.curSQLCommand.onFrontWriteFinished(mycatSs)) {
				mycatSs.curSQLCommand.clearResouces(false);
			}
		} else {
			MycatSession mycatSs = ((MySQLSession) session).getMycatSession();
			if (mycatSs.curSQLCommand.onBackendWriteFinished((MySQLSession) session)) {
				mycatSs.curSQLCommand.clearResouces(false);
			}
		}

	}

}
