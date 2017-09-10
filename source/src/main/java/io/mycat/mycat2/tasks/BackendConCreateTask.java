package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;

import io.mycat.mycat2.beans.MySQLMetaBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.AbstractMySQLSession.CurrPacketType;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.packet.AuthPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.BufferPool;
import io.mycat.util.CharsetUtil;
import io.mycat.util.SecurityUtil;

/**
 * 创建后端MySQL连接并负责完成登录认证的Processor
 * 
 * @author wuzhihui
 *
 */
public class BackendConCreateTask extends AbstractBackendIOTask<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);
	private HandshakePacket handshake;
	private boolean welcomePkgReceived = false;
	private MySQLMetaBean mySQLMetaBean;
	private SchemaBean schema;
	private MySQLSession session;

	public BackendConCreateTask(BufferPool bufPool, Selector nioSelector, MySQLMetaBean mySQLMetaBean, SchemaBean schema)
			throws IOException {
		String serverIP = mySQLMetaBean.getIp();
		int serverPort = mySQLMetaBean.getPort();
		logger.info("Connecting to backend MySQL Server " + serverIP + ":" + serverPort);
		InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
		SocketChannel backendChannel = SocketChannel.open();
		backendChannel.configureBlocking(false);
		backendChannel.connect(serverAddress);
		session = new MySQLSession(bufPool, nioSelector, backendChannel);
		session.setMySQLMetaBean(mySQLMetaBean);
		this.setSession(session, false);
		this.mySQLMetaBean = mySQLMetaBean;
		this.schema = schema;

	}

	@Override
	public void onSocketRead(MySQLSession session) throws IOException {
		session.proxyBuffer.reset();
		if (!session.readFromChannel() || CurrPacketType.Full != session.resolveMySQLPackage(session.proxyBuffer,
				session.curMSQLPackgInf, false)) {// 没有读到数据或者报文不完整
			return;
		}

		if (!welcomePkgReceived) {
			handshake = new HandshakePacket();
			handshake.read(this.session.proxyBuffer);

			// 设置字符集编码
			int charsetIndex = (handshake.serverCharsetIndex & 0xff);
			String charset = CharsetUtil.getCharset(charsetIndex);
			if (charset != null) {
				// conn.setCharset(charsetIndex, charset);
			} else {
				String errmsg = "Unknown charsetIndex:" + charsetIndex + " of " + session.getSessionId();
				logger.warn(errmsg);
				return;
			}
			// 发送应答报文给后端
			AuthPacket packet = new AuthPacket();
			packet.packetId = 1;
			packet.clientFlags = initClientFlags();
			packet.maxPacketSize = 1024 * 1000;
			packet.charsetIndex = charsetIndex;
			packet.user = mySQLMetaBean.getUser();
			try {
				packet.password = passwd(mySQLMetaBean.getPassword(), handshake);
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e.getMessage());
			}
			// SchemaBean schema = session.schema;
			if(schema!=null&&schema.getDefaultDN()!=null){
				packet.database = schema.getDefaultDN().getDatabase();
			}

			// 不透传的状态下，需要自己控制Buffer的状态，这里每次写数据都切回初始Write状态
			session.proxyBuffer.reset();
			packet.write(session.proxyBuffer);
			session.proxyBuffer.flip();
			// 不透传的状态下， 自己指定需要写入到channel中的数据范围
			// 没有读取,直接透传时,需要指定 透传的数据 截止位置
			session.proxyBuffer.readIndex = session.proxyBuffer.writeIndex;
			session.writeToChannel();
			welcomePkgReceived = true;
		} else {
			// 认证结果报文收到
			if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
				logger.info("backend authed suceess ");
				this.finished(true);
			} else if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {
				errPkg = new ErrorPacket();
				errPkg.read(session.proxyBuffer);
				logger.warn("backend authed failed. Err No. " + errPkg.errno + "," + errPkg.message);
				this.finished(false);
			}
		}
	}

	@Override
	public void onConnect(SelectionKey theKey, MySQLSession userSession, boolean success, String msg)
			throws IOException {
		String logInfo = success ? " backend connect success " : "backend connect failed " + msg;
		logger.info(logInfo + " channel " + userSession.channel);
		if (success) {
			InetSocketAddress serverRemoteAddr = (InetSocketAddress) userSession.channel.getRemoteAddress();
			InetSocketAddress serverLocalAddr = (InetSocketAddress) userSession.channel.getLocalAddress();
			userSession.addr = "local port:" + serverLocalAddr.getPort() + ",remote " + serverRemoteAddr.getHostString()
					+ ":" + serverRemoteAddr.getPort();
			userSession.channelKey.interestOps(SelectionKey.OP_READ);

		} else {
			errPkg = new ErrorPacket();
			errPkg.message = logInfo;
			finished(false);

		}
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
