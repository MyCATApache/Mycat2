package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLReplicatSet;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.DNBean;
import io.mycat.mycat2.beans.MySQLDataSource;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.packet.AuthPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.UserProxySession;
import io.mycat.proxy.UserProxySession.NetOptMode;
import io.mycat.util.CharsetUtil;
import io.mycat.util.SecurityUtil;

/**
 * 创建后端MySQL连接并负责完成登录认证的Processor
 * 
 * @author wuzhihui
 *
 */
public class BackendConCreateTask implements BackendIOTask<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);
	private ProxyBuffer prevFrontBuffer;
	private ProxyBuffer prevBackendBuffer;
	private NetOptMode prevNetMode;
	private NIOHandler<MySQLSession> prevProxyHandler;
	private final MySQLSession session;
	private HandshakePacket handshake;
	private boolean welcomePkgReceived = false;
	private AsynTaskCallBack callBack;
	private ErrorPacket errPkg;

	public BackendConCreateTask(MySQLSession session,MySQLDataSource ds) {
		
        
		prevNetMode = session.netOptMode;
		session.netOptMode = UserProxySession.NetOptMode.BackendRW;
		this.session = session;
		// 保存之前的FrontBuffer，BackendCon收到的数据会写入到session.frontBuffer中
		this.prevFrontBuffer = session.frontBuffer;
		this.prevBackendBuffer = session.backendBuffer;
		session.frontBuffer = session.allocNewProxyBuffer();
		session.backendBuffer = session.allocNewProxyBuffer();
		prevProxyHandler = session.curProxyHandler;
	}

	@Override
	public void onBackendRead(MySQLSession session) throws IOException {
		// 不透传的状态下，需要自己控制Buffer的状态，这里每次从Socket中读取并写Buffer数据都切回初始Write状态
		session.frontBuffer.reset();
		if (!session.readSocket(false)
				|| !session.resolveMySQLPackage(session.frontBuffer, session.curBackendMSQLPackgInf, false)) {// 没有读到数据或者报文不完整
			return;
		}

		if (!welcomePkgReceived) {
			handshake = new HandshakePacket();
			handshake.read(this.session.frontBuffer);

			// 设置字符集编码
			int charsetIndex = (handshake.serverCharsetIndex & 0xff);
			String charset = CharsetUtil.getCharset(charsetIndex);
			if (charset != null) {
				// conn.setCharset(charsetIndex, charset);
			} else {
				String errmsg = "Unknown charsetIndex:" + charsetIndex + " of " + session.sessionId;
				logger.warn(errmsg);
				return;
			}
			// 发送应答报文给后端
			String user = "root";
			String password = "123456";
			String schema = "mysql";
			AuthPacket packet = new AuthPacket();
			packet.packetId = 1;
			packet.clientFlags = initClientFlags();
			packet.maxPacketSize = 1024 * 1000;
			packet.charsetIndex = charsetIndex;
			packet.user = user;
			try {
				packet.password = passwd(password, handshake);
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e.getMessage());
			}
			packet.database = schema;

			// 不透传的状态下，需要自己控制Buffer的状态，这里每次写数据都切回初始Write状态
			session.backendBuffer.reset();
			packet.write(session.backendBuffer);
			session.backendBuffer.flip();
			session.writeToChannel(session.backendBuffer, session.backendChannel);
			welcomePkgReceived = true;
		} else {
			// 认证结果报文收到
			if (session.curBackendMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
				logger.info("backend authed suceess ");
				this.finished(true);
			} else if (session.curBackendMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {
				errPkg = new ErrorPacket();
				errPkg.read(session.frontBuffer);
				logger.warn("backend authed failed. Err No. " + errPkg.errno + "," + errPkg.message);
				this.finished(false);
			}
		}
	}

	@Override
	public void onBackendWrite(MySQLSession session) throws IOException {
	}

	@Override
	public void onBackendSocketClosed(MySQLSession userSession, boolean normal) {

	}

	@Override
	public void onBackendConnect(MySQLSession userSession, boolean success, String msg) throws IOException {
		String logInfo = success ? " backend connect success " : "backend connect failed " + msg;
		logger.info(logInfo + " channel " + userSession.backendChannel);
		if (success) {
			InetSocketAddress serverRemoteAddr = (InetSocketAddress) userSession.backendChannel.getRemoteAddress();
			InetSocketAddress serverLocalAddr = (InetSocketAddress) userSession.backendChannel.getLocalAddress();
			userSession.backendAddr = "local port:" + serverLocalAddr.getPort() + ",remote "
					+ serverRemoteAddr.getHostString() + ":" + serverRemoteAddr.getPort();
			userSession.backendKey = userSession.backendChannel.register(userSession.nioSelector, SelectionKey.OP_READ,
					userSession);

		} else {
			errPkg = new ErrorPacket();
			errPkg.message = logInfo;
			finished(false);

		}
	}

	private void finished(boolean success) throws IOException {
		sessionRecover();
		if (!success) {
			session.backendChannel = null;
			session.backendKey = null;
		}
		callBack.finished(session, this, success, this.errPkg);
	}

	public void sessionRecover() {
		// 释放先前分配的资源
		session.recycleAllocedBuffer(session.frontBuffer);
		session.recycleAllocedBuffer(session.backendBuffer);
		// 恢复Session原来的状态
		session.frontBuffer = prevFrontBuffer;
		session.backendBuffer = prevBackendBuffer;
		session.netOptMode = prevNetMode;
		session.curProxyHandler = prevProxyHandler;
	}

	private static byte[] passwd(String pass, HandshakePacket hs) throws NoSuchAlgorithmException {
		if (pass == null || pass.length() == 0) {
			return null;
		}
		byte[] passwd = pass.getBytes();
		int sl1 = hs.seed.length;
		int sl2 = hs.restOfScrambleBuff.length;
		byte[] seed = new byte[sl1 + sl2];
		System.arraycopy(hs.seed, 0, seed, 0, sl1);
		System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
		return SecurityUtil.scramble411(passwd, seed);
	}

	@Override
	public void setCallback(AsynTaskCallBack callBack) {
		this.callBack = callBack;

	}

	private static long initClientFlags() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		boolean usingCompress = false;
		if (usingCompress) {
			flag |= Capabilities.CLIENT_COMPRESS;
		}
		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= Capabilities.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		// client extension
		flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
		flag |= Capabilities.CLIENT_MULTI_RESULTS;
		return flag;
	}

}
