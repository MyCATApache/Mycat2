package io.mycat.mycat2.cmd;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.AuthPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.CharsetUtil;

public class ClientAuthProcessor extends AbstractSQLProcessor {
	private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };
	private static Logger logger = LoggerFactory.getLogger(ClientAuthProcessor.class);
	public static ClientAuthProcessor INSTANCE = new ClientAuthProcessor();
	// private ArrayList<Runnable> pendingJob;

	@Override
	public void handFrontPackage(MySQLSession session) throws IOException {
		boolean readed = readPackage(session,true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false || session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf) == false) {
			return;
		}
		try {
			backendBuffer.readState.optPostion = 0;
			AuthPacket auth = new AuthPacket();
			auth.read(backendBuffer);

			// Fake check user
			logger.debug("Check user name. " + auth.user);
			// if (!auth.user.equals("root")) {
			// LOGGER.debug("User name error. " + auth.user);
			// mySQLFrontConnection.failure(ErrorCode.ER_ACCESS_DENIED_ERROR,
			// "Access denied for user '" + auth.user + "'");
			// mySQLFrontConnection.getProtocolStateMachine().setNextState(BackendCloseState.INSTANCE);
			// return true;
			// }

			// Fake check password
			logger.debug("Check user password. " + new String(auth.password));

			// check schema
			logger.debug("Check database. " + auth.database);

			boolean succ = success(auth);
			if (succ) {
				session.answerFront(AUTH_OK);
				return;
			}
		} catch (Throwable e) {
			logger.warn("Frontend FrontendAuthenticatingState error:", e);
		}

	}

	private boolean success(AuthPacket auth) throws IOException {
		logger.debug("Login success");
		// 设置字符集编码
		int charsetIndex = (auth.charsetIndex & 0xff);
		final String charset = CharsetUtil.getCharset(charsetIndex);
		if (charset == null) {
			final String errmsg = "Unknown charsetIndex:" + charsetIndex;
			logger.warn(errmsg);
			// con.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, errmsg);
			// con.getProtocolStateMachine().setNextState(BackendCloseState.INSTANCE);
			return true;
		}
		logger.debug("charset = {}, charsetIndex = {}", charset, charsetIndex);

		// con.setCharset(charsetIndex, charset);

		// String db = StringUtil.isEmpty(auth.database)
		// ? SQLEngineCtx.INSTANCE().getDefaultMycatSchema().getName() :
		// auth.database;
		// if (!con.setFrontSchema(db)) {
		// final String errmsg = "No Mycat Schema defined: " + auth.database;
		// return.debug(errmsg);
		// con.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "42000".getBytes(),
		// errmsg);
		// con.setWriteCompleteListener(() -> {
		// con.getProtocolStateMachine().setNextState(BackendCloseState.INSTANCE);
		// con.getNetworkStateMachine().setNextState(ClosingState.INSTANCE);
		// });
		// } else {
		// con.getDataBuffer().writeBytes(AUTH_OK);
		//
		// }
		return true;
	}

	@Override
	public void handBackendPackage(MySQLSession session) throws IOException {
		boolean readed = readPackage(session,false);
		ProxyBuffer frontBuffer = session.frontBuffer;
		if (readed == false || session.resolveMySQLPackage(frontBuffer, session.curBackendMSQLPackgInf) == false) {
			return;
		}
		while (frontBuffer.readState.hasRemain()) {
			boolean resolved = session.resolveMySQLPackage(frontBuffer, session.curBackendMSQLPackgInf);
			if (!resolved) {
				logger.warn("has half pakcage ");

				break;
			}
		}
		session.writeToChannel(frontBuffer, session.frontChannel);
		return;
	}

}
