package io.mycat.mycat2.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import io.mycat.mycat2.beans.conf.FireWallBean;
import io.mycat.mycat2.beans.conf.UserBean;
import io.mycat.mycat2.beans.conf.UserConfig;
import io.mycat.mysql.Alarms;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.ConfigEnum;
import io.mycat.util.ErrorCode;
import io.mycat.util.SecurityUtil;
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

		// 处理用户认证报文
		try {
			AuthPacket auth = new AuthPacket();
			auth.read(frontBuffer);

			MycatConfig config = ProxyRuntime.INSTANCE.getConfig();
			UserConfig userConfig = config.getConfig(ConfigEnum.USER);
			UserBean userBean = null;
			for (UserBean user : userConfig.getUsers()) {
				if (user.getName().equals(auth.user)) {
					userBean = user;
					break;
				}
			}

			// check user
			if (!checkUser(session, userConfig, userBean)) {
				failure(session, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "' with addr '" + session.addr + "'");
				return;
			}

			// check password
			if (!checkPassword(session, userBean, auth.password)) {
				failure(session, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "', because password is error ");
				return;
			}

			// check degrade
//			if (isDegrade(auth.user)) {
//				failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "', because service be degraded ");
//				return;
//			}

			// check schema
			switch (checkSchema(userBean, auth.database)) {
				case ErrorCode.ER_BAD_DB_ERROR:
					failure(session, ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + auth.database + "'");
					break;
				case ErrorCode.ER_DBACCESS_DENIED_ERROR:
					String s = "Access denied for user '" + auth.user + "' to database '" + auth.database + "'";
					failure(session, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
					break;
				default:
					// set schema
					if (auth.database == null) {
						session.schema = (userBean.getSchemas() == null) ?
								config.getDefaultSchemaBean() : config.getSchemaBean(userBean.getSchemas().get(0));
					} else {
						session.schema = config.getSchemaBean(auth.database);
					}

					logger.debug("set schema: {} for user: {}", session.schema, auth.user);
					if (success(session, auth)) {
						session.clientUser=auth.user;
						session.proxyBuffer.reset();
						session.answerFront(AUTH_OK);
						// 认证通过，设置当前SQL Handler为默认Handler
						session.setCurNIOHandler(DefaultMycatSessionHandler.INSTANCE);
					}
			}
		} catch (Throwable e) {
			logger.warn("Frontend FrontendAuthenticatingState error:", e);
		}
	}

	private boolean checkUser(MycatSession session, UserConfig userConfig, UserBean userBean) throws IOException {
		if (userBean == null) {
			return false;
		}

		FireWallBean firewall = userConfig.getFirewall();
		if (!firewall.isEnable()) {
			return true;
		}

		// 防火墙 白名单处理
		boolean isPassed = false;
		List<FireWallBean.WhiteBean> whiteBeanList = firewall.getWhite();

		if ((whiteBeanList != null && whiteBeanList.size() > 0)) {
			for (FireWallBean.WhiteBean whiteBean : whiteBeanList) {
				if (userBean.getName().equals(whiteBean.getUser()) && Pattern.matches(whiteBean.getHost(), session.host)) {
					isPassed = true;
					break;
				}
			}
		}

		if (!isPassed) {
			logger.error("firewall attack from host: {}, user: {}", session.host, userBean.getName());
			return false;
		}
		return true;
	}

	private boolean checkPassword(MycatSession session, UserBean userBean, byte[] password) {
		String pass = userBean.getPassword();

		// check null
		if (pass == null || pass.length() == 0) {
			return (password == null || password.length == 0);
		}

		if (password == null || password.length == 0) {
			return false;
		}

		// encrypt
		byte[] encryptPass;
		try {
			encryptPass = SecurityUtil.scramble411(pass.getBytes(), session.seed);
		} catch (NoSuchAlgorithmException e) {
			logger.warn("no such algorithm", e);
			return false;
		}

		if (encryptPass != null && (encryptPass.length == password.length)) {
			int i = encryptPass.length;
			while (i-- != 0) {
				if (encryptPass[i] != password[i]) {
					return false;
				}
			}
		} else {
			return false;
		}

		return true;
	}

	private int checkSchema(UserBean userBean, String schema) {
		if (schema == null) {
			return 0;
		}

		List<String> schemas = userBean.getSchemas();
		if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
			return 0;
		} else {
			return ErrorCode.ER_DBACCESS_DENIED_ERROR;
		}
	}

	private void failure(MycatSession session, int errno, String info) throws IOException {
		ErrorPacket errorPacket = new ErrorPacket();
		errorPacket.packetId = 2;
		errorPacket.errno = errno;
		errorPacket.message = info;
		session.responseOKOrError(errorPacket);
	}

	private boolean success(MycatSession session, AuthPacket auth) throws IOException {
		// 设置字符集编码
		int charsetIndex = (auth.charsetIndex & 0xff);
		// 保存字符集索引
		session.charSet.charsetIndex = charsetIndex;
//		ProxyRuntime.INSTANCE.getConfig().getMySQLRepBean(session.schema.getDefaultDN().getReplica()).getMetaBeans().get(0).INDEX_TO_CHARSET.get(charsetIndex);
		logger.debug("login success, charsetIndex = {}", charsetIndex);
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
