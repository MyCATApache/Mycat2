package io.mycat.mycat2.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mysql.MysqlNativePasswordPluginUtil;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.NewAuthPacket;
import io.mycat.mysql.packet.NewHandshakePacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ErrorCode;

/**
 * 创建后端MySQL连接并负责完成登录认证的Processor
 * 
 * @author wuzhihui
 */
public class BackendConCreateTask extends AbstractBackendIOTask<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);
	private NewHandshakePacket handshake;
	private boolean welcomePkgReceived = false;
	private final MySQLMetaBean mySQLMetaBean;
	private final SchemaBean schema;
	private MySQLSession session;
	private final boolean addConnectionPool;

	/**
	 * 异步非阻塞模式创建MySQL连接，如果连接创建成功，需要把新连接加入到所在ReactorThread的连接池，则参数addConnectionPool需要设置为True
	 * 
	 * @param bufPool
	 * @param nioSelector
	 *            在哪个Selector上注册NIO事件（即对应哪个ReactorThread）
	 * @param mySQLMetaBean
	 * @param schema
	 * @param callBack
	 *            创建连接结束（成功或失败）后的回调接口
	 * @param addConnectionPool
	 * @throws IOException
	 */
	public BackendConCreateTask(BufferPool bufPool, Selector nioSelector, MySQLMetaBean mySQLMetaBean,
			SchemaBean schema, AsynTaskCallBack<MySQLSession> callBack, boolean addConnectionPool) throws IOException {
		String serverIP = mySQLMetaBean.getDsMetaBean().getIp();
		int serverPort = mySQLMetaBean.getDsMetaBean().getPort();
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
		this.callBack = callBack;
		this.addConnectionPool = addConnectionPool;
	}

	@Override
	public void onSocketRead(MySQLSession session) throws IOException {
		session.proxyBuffer.reset();
		if (!session.readFromChannel() || CurrPacketType.Full != session.resolveMySQLPackage(false)) {
			// 没有读到数据或者报文不完整
			return;
		}

		if (MySQLPacket.ERROR_PACKET == session.curMSQLPackgInf.pkgType) {
			errPkg = new ErrorPacket();
			MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
			session.proxyBuffer.readIndex = curMQLPackgInf.startPos;
			errPkg.read(session.proxyBuffer);
			logger.warn("backend authed failed. Err No. " + errPkg.errno + "," + errPkg.message);
			this.finished(false);
			return;
		}

		if (!welcomePkgReceived) {
			handshake = new NewHandshakePacket();
			handshake.read(this.session.proxyBuffer);

			// 设置字符集编码
//			int charsetIndex = (handshake.characterSet & 0xff);
			int charsetIndex = handshake.characterSet;
			// 发送应答报文给后端
			NewAuthPacket packet = new NewAuthPacket();
			packet.packetId = 1;
			packet.capabilities = MySQLSession.getClientCapabilityFlags().value;
			packet.maxPacketSize = 1024 * 1000;
			packet.characterSet = (byte) charsetIndex;
			packet.username = mySQLMetaBean.getDsMetaBean().getUser();
			packet.password = MysqlNativePasswordPluginUtil.scramble411(mySQLMetaBean.getDsMetaBean().getPassword(), handshake.authPluginDataPartOne + handshake.authPluginDataPartTwo);
			packet.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;
			// SchemaBean mycatSchema = session.mycatSchema;
			// 创建连接时，默认不主动同步数据库
			// if(mycatSchema!=null&&mycatSchema.getDefaultDN()!=null){
			// packet.database = mycatSchema.getDefaultDN().getDatabase();
			// }

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
				logger.debug("backend authed suceess ");
				this.finished(true);
			}
		}
	}

	protected void onFinished(boolean success) {
		super.onFinished(success);
		if (success && addConnectionPool) {
			logger.debug("add new created mysql session to reactorthread pool {} ", this.session);
			((MycatReactorThread) Thread.currentThread()).addNewMySQLSession(this.session);
		}
	}

	@Override
	public void onConnect(SelectionKey theKey, MySQLSession userSession, boolean success, String msg)
			throws IOException {
		if (success) {
			InetSocketAddress serverRemoteAddr = (InetSocketAddress) userSession.channel.getRemoteAddress();
			InetSocketAddress serverLocalAddr = (InetSocketAddress) userSession.channel.getLocalAddress();
			userSession.addr = "local port:" + serverLocalAddr.getPort() + ",remote " + serverRemoteAddr.getHostString()
					+ ":" + serverRemoteAddr.getPort();
			userSession.channelKey.interestOps(SelectionKey.OP_READ);
		} else {
			errPkg = new ErrorPacket();
			errPkg.packetId = 1;
			errPkg.errno = ErrorCode.ERR_CONNECT_SOCKET;
			errPkg.message = "backend connect failed " + msg;
			finished(false);
		}
		if (logger.isDebugEnabled()) {
			String logInfo = success ? " backend connect success " : "backend connect failed " + msg;
			logger.debug("{}  sessionId = {}, {}:{}", logInfo, userSession.getSessionId(),
					userSession.getMySQLMetaBean().getDsMetaBean().getIp(),
					userSession.getMySQLMetaBean().getDsMetaBean().getPort());
		}
	}

}
