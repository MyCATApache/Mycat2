package io.mycat.mycat2.net;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MySQLSession.CurrPacketType;
import io.mycat.mysql.packet.AuthPacket;
import io.mycat.proxy.DefaultDirectProxyHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.CharsetUtil;

/**
 * MySQL客户端登录认证的Handler，为第一个Handler
 *
 * @author wuzhihui
 *
 */
public class MySQLClientAuthHandler extends DefaultDirectProxyHandler<MySQLSession> {
	private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };


	public static final  MySQLClientAuthHandler INSTANCE=new MySQLClientAuthHandler();

	@Override
	public void onFrontRead(MySQLSession session) throws IOException {
		ProxyBuffer frontBuffer = session.frontBuffer;
		if(session.readFromChannel(session.frontBuffer, session.frontChannel)==false||
				CurrPacketType.Full!=session.resolveMySQLPackage(frontBuffer, session.curFrontMSQLPackgInf, false)){
			return;
		}

		//处理用户认证请情况报文
		try {
			AuthPacket auth = new AuthPacket();
			auth.read(frontBuffer);
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
				session.frontBuffer.reset();
				session.answerFront(AUTH_OK);
				//认证通过，设置当前SQL Handler为默认Handler
				session.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
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


}
