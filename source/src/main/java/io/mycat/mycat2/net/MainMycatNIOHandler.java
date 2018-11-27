package io.mycat.mycat2.net;

import static io.mycat.mycat2.cmds.LoadDataState.CLIENT_2_SERVER_EMPTY_PACKET;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.ComQuitCmd;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.LoadDataCommand;
import io.mycat.mycat2.cmds.manager.MyCatCmdDispatcher;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;
import io.mycat.util.LoadDataUtil;

/**
 * 负责MycatSession的NIO事件，驱动SQLCommand命令执行，完成SQL的处理过程
 * Mycat用户登录阶段通过以后，NIO事件主要由他来响应，包括派发到具体的SQLCommand。
 *
 * @author wuzhihui
 */
public class MainMycatNIOHandler implements NIOHandler<MycatSession> {
	public static final MainMycatNIOHandler INSTANCE = new MainMycatNIOHandler();
	private static Logger logger = LoggerFactory.getLogger(MainMycatNIOHandler.class);

	public void onSocketRead(final MycatSession session) throws IOException {
		boolean readed = session.readFromChannel();
		if (!readed)
			return;
		MySQLCommand curCmd = session.getCurSQLCommand();
		if (curCmd == null) {
			// if (session.getCurSQLCommand() == LoadDataCommand.INSTANCE) {
			// resolveLoadData(session);
			// return;
			// } else
			CurrPacketType currPacketType = session.resolveMySQLPackage(false, true);
			if (CurrPacketType.Full == currPacketType) {
				session.changeToDirectIfNeed();
			} else if (CurrPacketType.LongHalfPacket == currPacketType
					|| CurrPacketType.ShortHalfPacket == currPacketType) {
				if (!resolveHalfPackage(session))
					return;
				session.proxyBuffer.readMark = session.proxyBuffer.readIndex;
				return;
			}
			ProxyBuffer buffer = session.getProxyBuffer();
			if (session.curMSQLPackgInf.endPos < buffer.writeIndex) {
				logger.warn("front contains multi package ");
			}
			processSQL(session);
		} else {
			//当前的SQLCommand没有处理完请求，继续处理
			if (curCmd.procssSQL(session)) {
				curCmd.clearResouces(session, session.isClosed());
				session.switchSQLCommand(null);
			}
		}
	}

	private void processSQL(final MycatSession session) throws IOException {
		switch (session.curMSQLPackgInf.pkgType) {
		case MySQLCommand.COM_QUERY: {
			doQuery(session);
			return;
		}
		case MySQLCommand.COM_QUIT: {
		  session.switchSQLCommand(ComQuitCmd.INSTANCE);
		  break;
		}
		default: {
			session.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
			break;
		}
		}
		 if(session.getCurSQLCommand().procssSQL(session)){
           session.getCurSQLCommand().clearResouces(session, session.isClosed());
           session.switchSQLCommand(null);
         }
		// if (!delegateRoute(session)) {
		// return false;
		// }

		// /**
		// * 设置原始处理命令
		// * 1. 设置目标命令
		// * 2. 处理动态注解
		// * 3. 处理静态注解
		// * 4. 构建命令或者注解链。 如果没有注解链，直接返回目标命令
		// */
		// SQLAnnotationChain chain = new SQLAnnotationChain();
		// session.curSQLCommand =
		// chain.setTarget(command).processDynamicAnno(session)
		// .processStaticAnno(session, staticAnnontationMap).build();
	}

	private void doQuery(final MycatSession session) throws IOException {
		MySQLCommand command;

		int rowDataIndex = session.curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize + 1;
		int length = session.curMSQLPackgInf.pkgLength - MySQLPacket.packetHeaderSize - 1;
		try {
			session.parser.parse(session.proxyBuffer.getBuffer(), rowDataIndex, length, session.sqlContext);
		} catch (Exception e) {
			try {
				logger.error("sql parse error", e);
				session.sendErrorMsg(ErrorCode.ER_PARSE_ERROR, "sql parse error : " + e.getMessage());
			} catch (Exception e1) {
				session.close(false, e1.getMessage());
			}
			return;
		}
		byte sqltype = session.sqlContext.getSQLType() != 0 ? session.sqlContext.getSQLType()
				: session.sqlContext.getCurSQLType();
		session.setSqltype(sqltype);
		switch (sqltype) {
		case BufferSQLContext.MYCAT_SQL:
			command = MyCatCmdDispatcher.INSTANCE.getMycatCommand(session.sqlContext);
			break;
		case BufferSQLContext.ANNOTATION_SQL:
			command = MyCatCmdDispatcher.INSTANCE.getMycatCommand(session.sqlContext);
			break;
		case BufferSQLContext.SET_AUTOCOMMIT_SQL:
		case BufferSQLContext.START_TRANSACTION_SQL:
		case BufferSQLContext.XA_BEGIN:
			logger.debug("received transaction sql,type {}", sqltype);
			// @todo transaction status ??
			command = DirectPassthrouhCmd.INSTANCE;
			break;
		case BufferSQLContext.LOAD_SQL:
			command = LoadDataCommand.INSTANCE;
			break;
		default:
			command = DirectPassthrouhCmd.INSTANCE;
			break;
		}
		session.switchSQLCommand(command);
		if (command.procssSQL(session)) {
			command.clearResouces(session, session.isClosed());
			session.switchSQLCommand(null);
		}
	}

	private boolean resolveHalfPackage(MycatSession session) throws IOException {
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
				return false;
			}
		}
		return true;
	}

	private void resolveLoadData(final MycatSession mycatSession) throws IOException {
		LoadDataUtil.readOverByte(mycatSession, mycatSession.proxyBuffer);
		if (LoadDataUtil.checkOver(mycatSession)) {
			LoadDataUtil.change2(mycatSession, CLIENT_2_SERVER_EMPTY_PACKET);
		}
		mycatSession.proxyBuffer.flip();
		mycatSession.proxyBuffer.readIndex = mycatSession.proxyBuffer.writeIndex;
		mycatSession.giveupOwner(SelectionKey.OP_WRITE);
		mycatSession.getCurBackend().writeToChannel();
	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 *
	 * @param session
	 * @param normal
	 */
	public void onSocketClosed(MycatSession session, boolean normal) {
		logger.info("front socket closed " + session);
		session.lazyCloseSession(normal, "front closed");
	}

	@Override
	public void onSocketWrite(MycatSession session) throws IOException {
		session.writeToChannel();

	}

	@Override
	public void onConnect(SelectionKey curKey, MycatSession session, boolean success, String msg) {
		throw new java.lang.RuntimeException("not implemented ");
	}

	@Override
	public void onWriteFinished(MycatSession session) throws IOException {
		logger.debug("write finished  {}", this);
		MySQLCommand curCmd = session.getCurSQLCommand();
		if (curCmd != null) {
			// 交给SQLComand去处理
			if (curCmd.onFrontWriteFinished(session)) {
				curCmd.clearResouces(session, false);
				session.switchSQLCommand(null);
			}
		}

	}

}
