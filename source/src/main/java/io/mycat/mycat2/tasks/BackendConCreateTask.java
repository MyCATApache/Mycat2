package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.packet.*;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.ErrorCode;
import io.mycat.util.ParseUtil;
import io.mycat.util.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;

/**
 * 创建后端MySQL连接并负责完成登录认证的Processor
 *
 * @author wuzhihui
 */
public class BackendConCreateTask extends AbstractBackendIOTask<MySQLSession> {
    private static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);
    private HandshakePacket handshake;
    private boolean welcomePkgReceived = false;
    private MySQLMetaBean mySQLMetaBean;
    private SchemaBean schema;
    private MySQLSession session;

    public BackendConCreateTask(BufferPool bufPool, Selector nioSelector, MySQLMetaBean mySQLMetaBean, SchemaBean schema, AsynTaskCallBack<MySQLSession> callBack)
            throws IOException {
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
//        // client extension
//        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
//        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;
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
            errPkg.packetId = session.proxyBuffer.getByte(session.curMSQLPackgInf.startPos
                    + ParseUtil.mysql_packetHeader_length);
            errPkg.read(session.proxyBuffer);
            logger.warn("backend authed failed. Err No. " + errPkg.errno + "," + errPkg.message);
            this.finished(false);
            return;
        }

        if (!welcomePkgReceived) {
            handshake = new HandshakePacket();
            handshake.read(this.session.proxyBuffer);

            // 设置字符集编码
            int charsetIndex = (handshake.serverCharsetIndex & 0xff);
            // 发送应答报文给后端
            AuthPacket packet = new AuthPacket();
            packet.packetId = 1;
            packet.clientFlags = initClientFlags();
            packet.maxPacketSize = 1024 * 1000;
            packet.charsetIndex = charsetIndex;
            packet.user = mySQLMetaBean.getDsMetaBean().getUser();
            try {
                packet.password = passwd(mySQLMetaBean.getDsMetaBean().getPassword(), handshake);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e.getMessage());
            }
            // SchemaBean mycatSchema = session.mycatSchema;
            // 创建连接时，默认不主动同步数据库
//			if(mycatSchema!=null&&mycatSchema.getDefaultDN()!=null){
//				packet.database = mycatSchema.getDefaultDN().getDatabase();
//			}

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
            logger.debug("{}  sessionId = {}, {}:{}", logInfo, userSession.getSessionId(), userSession.getMySQLMetaBean().getDsMetaBean().getIp(), userSession.getMySQLMetaBean().getDsMetaBean().getPort());
        }
    }


}
