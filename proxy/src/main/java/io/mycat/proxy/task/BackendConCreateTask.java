/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.task;

import io.mycat.MycatExpection;
import io.mycat.config.GlobalConfig;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.AuthPacketImpl;
import io.mycat.proxy.packet.HandshakePacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackendConCreateTask implements NIOHandler<MySQLClientSession> {
    final MySQLDatasource datasource;
    final AsynTaskCallBack<MySQLClientSession> callback;
    boolean welcomePkgReceived = false;
    protected final static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);

    public BackendConCreateTask(MySQLDatasource datasource, MySQLSessionManager sessionManager, MycatReactorThread curThread, AsynTaskCallBack<MySQLClientSession> callback) throws IOException {
        this.datasource = datasource;
        this.callback = callback;
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        MySQLClientSession mySQLSession = new MySQLClientSession(datasource, curThread.getSelector(), channel, SelectionKey.OP_CONNECT, this, sessionManager);
        mySQLSession.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
        channel.connect(new InetSocketAddress(datasource.getIp(), datasource.getPort()));
    }

    @Override
    public void onConnect(SelectionKey curKey, MySQLClientSession mysql, boolean success, Throwable throwable) throws IOException {
        if (success) {
            mysql.change2ReadOpts();
        } else {
            String message = throwable.getMessage();
            mysql.close(true, message);
            callback.finished(null, this, false, null, message);
        }
    }

    @Override
    public void onSocketRead(MySQLClientSession mysql) throws IOException {
        ProxyBuffer proxyBuffer = mysql.currentProxyBuffer().newBufferIfNeed();
        if (this != mysql.getCurNIOHandler()) {
            return;
        }
        if (!mysql.readFromChannel()){
            return;
        }
        if (!mysql.readProxyPayloadFully()) {
            return;
        }
        if (!welcomePkgReceived) {
            int serverCapabilities = GlobalConfig.getClientCapabilityFlags().value;
            mysql.getPacketResolver().setCapabilityFlags(serverCapabilities);

            HandshakePacketImpl hs = new HandshakePacketImpl();
            if ((proxyBuffer.get(4) & 0xff) == 0xff) {
                throw new MycatExpection("receive error packet");
            }
            hs.readPayload(mysql.currentProxyPayload());
            mysql.resetCurrentProxyPayload();
            int charsetIndex = hs.characterSet;
            AuthPacketImpl packet = new AuthPacketImpl();
            packet.capabilities = serverCapabilities;
            packet.maxPacketSize = 1024 * 1000;
            packet.characterSet = (byte) charsetIndex;
            packet.username = datasource.getUsername();
            packet.password = MysqlNativePasswordPluginUtil.scramble411(datasource.getPassword(), hs.authPluginDataPartOne + hs.authPluginDataPartTwo);
            packet.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;
            MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(1024);
            packet.writePayload(mySQLPacket);
            welcomePkgReceived = true;
            mysql.writeProxyPacket(mySQLPacket, 1);
        } else {
            if (mysql.getPayloadType() == MySQLPayloadType.FIRST_OK) {
                mysql.resetPacket();
                mysql.setCurrentProxyBuffer(null);
                callback.finished(mysql, this, true, null, null);
            } else {
                callback.finished(null, this, false, null, "create mysql backend fail");
            }
        }

    }

    @Override
    public void onWriteFinished(MySQLClientSession mysql) throws IOException {
        mysql.change2ReadOpts();
    }

    @Override
    public void onSocketClosed(MySQLClientSession session, boolean normal, String reasion) {
        callback.finished(session, this, normal, null, reasion);
    }


}
