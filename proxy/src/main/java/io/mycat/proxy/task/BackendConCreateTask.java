package io.mycat.proxy.task;

import io.mycat.beans.MySQLMeta;
import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.MysqlNativePasswordPluginUtil;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.AuthPacketImpl;
import io.mycat.proxy.packet.HandshakePacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class BackendConCreateTask implements NIOHandler<MySQLSession> {
    final Datasource datasource;
    final AsynTaskCallBack<MySQLSession> callback;
    boolean welcomePkgReceived = false;
    protected final static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);

    public BackendConCreateTask(Datasource datasource, MySQLSessionManager sessionManager, MycatReactorThread curThread, AsynTaskCallBack<MySQLSession> callback) throws IOException {
        this.datasource = datasource;
        this.callback = callback;
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        MySQLSession mySQLSession = new MySQLSession(datasource, curThread.getSelector(), channel, SelectionKey.OP_CONNECT, this, sessionManager);
        mySQLSession.setProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
        channel.connect(new InetSocketAddress(datasource.getIp(), datasource.getPort()));
    }

    @Override
    public void onConnect(SelectionKey curKey, MySQLSession mysql, boolean success, Throwable throwable) throws IOException {
        if (success) {
            mysql.change2ReadOpts();
        } else {
            String message = throwable.getMessage();
            mysql.close(true, message);
            callback.finished(null, this, false, null, message);
        }
    }

    @Override
    public void onSocketRead(MySQLSession mysql) throws IOException {
        ProxyBuffer proxyBuffer = mysql.currentProxyBuffer().newBufferIfNeed();
        if (this != mysql.getCurNIOHandler()) {
            return;
        }
        if (!mysql.readFromChannel()){
            return;
        }
        if (!mysql.readMySQLPayloadFully()) {
            return;
        }
        if (!welcomePkgReceived) {
            HandshakePacketImpl hs = new HandshakePacketImpl();
            if ((proxyBuffer.get(4) & 0xff) == 0xff) {
                throw new MycatExpection("receive error packet");
            }
            hs.readPayload(mysql.currentPayload());
            mysql.resetCurrentPayload();
            int charsetIndex = hs.characterSet;
            AuthPacketImpl packet = new AuthPacketImpl();
            packet.capabilities = MySQLMeta.getClientCapabilityFlags().value;
            packet.maxPacketSize = 1024 * 1000;
            packet.characterSet = (byte) charsetIndex;
            packet.username = datasource.getUsername();
            packet.password = MysqlNativePasswordPluginUtil.scramble411(datasource.getPassword(), hs.authPluginDataPartOne + hs.authPluginDataPartTwo);
            packet.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;
            MySQLPacket mySQLPacket = mysql.newCurrentMySQLPacket();
            packet.writePayload(mySQLPacket);
            welcomePkgReceived = true;
            mysql.writeMySQLPacket(mySQLPacket, 1);
        } else {
            if (mysql.getPayloadType() == MySQLPayloadType.OK) {
                mysql.resetPacket();
                mysql.setProxyBuffer(null);
                callback.finished(mysql, this, true, null, null);
            } else {
                callback.finished(null, this, false, null, "create mysql backend fail");
            }
        }

    }

    @Override
    public void onWriteFinished(MySQLSession mysql) throws IOException {
        mysql.change2ReadOpts();
    }

    @Override
    public void onSocketClosed(MySQLSession mysql, boolean normal) {

    }

}
