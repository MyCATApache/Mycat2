package io.mycat.mycat2.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.pkgread.CommandHandlerAdapter;
import io.mycat.mycat2.cmds.pkgread.HandlerAdapter;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;

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
				&& readed == false) {
			return;
		}

		switch (session.resolveMySQLPackage(buffer, session.curMSQLPackgInf, false)) {
		case Full:
			session.changeToDirectIfNeed();
			break;
		case LongHalfPacket:
			// 解包获取包的数据长度
			int pkgLength = session.curMSQLPackgInf.pkgLength;
			ByteBuffer bytebuffer = session.proxyBuffer.getBuffer();
			if (pkgLength > bytebuffer.capacity() && !bytebuffer.hasRemaining()) {
				try {
					session.ensureFreeSpaceOfReadBuffer();
				} catch (RuntimeException e1) {
					if (!session.curMSQLPackgInf.crossBuffer) {
						session.curMSQLPackgInf.crossBuffer = true;
						session.curMSQLPackgInf.remainsBytes = pkgLength
								- (session.curMSQLPackgInf.endPos - session.curMSQLPackgInf.startPos);
						session.sendErrorMsg(ErrorCode.ER_UNKNOWN_ERROR, e1.getMessage());
					}
					session.proxyBuffer.readIndex = session.proxyBuffer.writeIndex;
				}
			}
		case ShortHalfPacket:
			session.proxyBuffer.readMark = session.proxyBuffer.readIndex;
			return;
		}

		if (session.curMSQLPackgInf.endPos < buffer.writeIndex) {
			logger.warn("front contains multi package ");
		}

		// 进行后端的结束报文处理的绑定
		CommandHandlerAdapter adapter = HandlerAdapter.INSTANCE.getHandlerByType(session.curMSQLPackgInf.pkgType);

		if (null == adapter) {
			logger.error("curr pkg Type :" + session.curMSQLPackgInf.pkgType + " is not handler proess");
			throw new IOException("curr pkgtype " + session.curMSQLPackgInf.pkgType + " not handler!");
		}

		// 指定session中的handler处理为指定的handler
		session.commandHandler = adapter;

		if (!session.matchMySqlCommand()) {
			return;
		}

		// 如果当前包需要处理，则交给对应方法处理，否则直接透传
		if (session.curSQLCommand.procssSQL(session)) {
			session.curSQLCommand.clearFrontResouces(session, session.isClosed());
		}
	}

	private void onBackendRead(MySQLSession session) throws IOException {
		// 交给SQLComand去处理
		MySQLCommand curCmd = session.getMycatSession().curSQLCommand;
		try {
			if (curCmd.onBackendResponse(session)) {
				curCmd.clearBackendResouces(session, session.isClosed());
			}
		} catch (ClosedChannelException ex) {
			String errmsg = " read backend response error ,backend conn has closed.";
			logger.error(errmsg);
			session.getMycatSession().closeBackendAndResponseError(session, false, ErrorCode.ERR_CONNECT_SOCKET,
					errmsg);
		} catch (IOException e) {
			logger.error(" read backend response error.", e);
			session.getMycatSession().closeBackendAndResponseError(session, false, ErrorCode.ERR_CONNECT_SOCKET,
					e.getMessage());
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
				mycatSs.curSQLCommand.clearFrontResouces(mycatSs, false);
			}
		} else {
			MycatSession mycatSs = ((MySQLSession) session).getMycatSession();
			if (mycatSs.curSQLCommand.onBackendWriteFinished((MySQLSession) session)) {
				mycatSs.curSQLCommand.clearBackendResouces((MySQLSession) session, false);
			}
		}
	}

}
