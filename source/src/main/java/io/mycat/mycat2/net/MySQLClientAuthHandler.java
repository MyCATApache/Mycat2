package io.mycat.mycat2.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.AbstractMySQLSession.CurrPacketType;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.MycatSession;
import io.mycat.mysql.packet.AuthPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;

/**
 * MySQL客户端登录认证的Handler，为第一个Handler
 *
 * @author wuzhihui
 *
 */
public class MySQLClientAuthHandler implements NIOHandler<MycatSession> {
	private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

	public static final MySQLClientAuthHandler INSTANCE = new MySQLClientAuthHandler();
	private static Logger logger = LoggerFactory.getLogger(MySQLClientAuthHandler.class);

	@Override
	public void onSocketRead(MycatSession session) throws IOException {
		ProxyBuffer frontBuffer = session.getProxyBuffer();
		if (session.readFromChannel() == false
				|| CurrPacketType.Full != session.resolveMySQLPackage(frontBuffer, session.curMSQLPackgInf, false)) {
			return;
		}

		// 处理用户认证请情况报文
		try {
			AuthPacket auth = new AuthPacket();
			auth.read(frontBuffer);
			// Fake check user
			logger.info("Check user name. " + auth.user);
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
			//TODO ...set schema
			MycatConfig mycatConf = ProxyRuntime.INSTANCE.getConfig();
			session.schema = mycatConf.getDefaultSchemaBean();
			boolean succ = success(session,auth);
			if (succ) {
				session.proxyBuffer.reset();
				session.answerFront(AUTH_OK);
				// 认证通过，设置当前SQL Handler为默认Handler
				session.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
			}
		} catch (Throwable e) {
			logger.warn("Frontend FrontendAuthenticatingState error:", e);
		}

	}

	private boolean success(MycatSession session, AuthPacket auth) throws IOException {
		logger.debug("Login success");
		// 设置字符集编码
		int charsetIndex = (auth.charsetIndex & 0xff);
		logger.debug("charsetIndex = {}", charsetIndex);
		//保存字符集索引
		session.charSet.charsetIndex = charsetIndex;

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
	public void onSocketWrite(MycatSession session) throws IOException {
		session.writeToChannel();

	}

	@Override
	public void onSocketClosed(MycatSession userSession, boolean normal) {
		userSession.lazyCloseSession(normal, "front closed");

	}

	@Override
	public void onConnect(SelectionKey curKey, MycatSession session, boolean success, String msg) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void onWriteFinished(MycatSession session) throws IOException {
		// 明确开启读操作
		session.proxyBuffer.flip();
		session.change2ReadOpts();

	}


}
