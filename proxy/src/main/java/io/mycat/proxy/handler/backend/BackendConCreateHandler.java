/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.handler.backend;

import io.mycat.GlobalConst;
import io.mycat.MycatException;
import io.mycat.beans.MySQLDatasource;
import io.mycat.beans.mysql.packet.*;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.callback.CommandCallBack;
import io.mycat.proxy.handler.BackendNIOHandler;
import io.mycat.proxy.handler.MySQLPacketExchanger;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolver.ComQueryState;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.SessionManager;
import io.mycat.util.CachingSha2PasswordPlugin;
import io.mycat.util.CharsetUtil;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Objects;

/**
 * @author jamie12221
 *  date 2019-05-10 22:24 向mysql服务器创建连接
 **/
public final class BackendConCreateHandler implements BackendNIOHandler<MySQLClientSession> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendConCreateHandler.class);
    final CommandCallBack callback;
    final String STR_CACHING_AUTH_STAGE = "FULL_AUTH";
    final MySQLDatasource datasource;
    boolean welcomePkgReceived = false;
    String seed = null;
    String stage = null;
    String mysqlVersion = null;
    String authPluginName = null;
    int charsetIndex;

    //todo
    public BackendConCreateHandler(MySQLDatasource datasource, MySQLSessionManager sessionManager,
                                   MycatReactorThread curThread, CommandCallBack callback) {
        Objects.requireNonNull(datasource);
        Objects.requireNonNull(sessionManager);
        Objects.requireNonNull(callback);
        this.datasource = datasource;
        this.callback = callback;
        MySQLClientSession mysql = new MySQLClientSession(SessionManager.nextSessionId(), datasource, this, sessionManager);
        mysql.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
        SocketChannel channel = null;
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            mysql.register(curThread.getSelector(), channel, SelectionKey.OP_CONNECT);
            InetSocketAddress inetSocketAddress = new InetSocketAddress(datasource.getIp(), datasource.getPort());
            channel.connect(inetSocketAddress);
            LOGGER.info("inetSocketAddress:{} ",inetSocketAddress);
        } catch (Exception e) {
            onException(mysql, e);
            callback.onFinishedException(null, e, null);
            return;
        }
    }

    @Override
    public void onConnect(SelectionKey curKey, MySQLClientSession mysql, boolean success,
                          Exception e) {
        if (success) {
            mysql.change2ReadOpts();
        } else {
            MycatMonitor.onBackendConCreateConnectException(mysql, e);
            onException(mysql, e);
            callback.onFinishedException(e, this, null);
        }
    }

    @Override
    public void onSocketRead(MySQLClientSession mysql) {
        try {
            ProxyBuffer proxyBuffer = mysql.currentProxyBuffer().newBufferIfNeed();
            assert (this == mysql.getCurNIOHandler());
            if (!mysql.readFromChannel()) {
                return;
            }
            handle(mysql);

        } catch (Exception e) {
            LOGGER.error("create mysql connection error {} {}", datasource, e.getMessage());
            MycatMonitor.onBackendConCreateReadException(mysql, e);
            onException(mysql, e);
            callback.onFinishedException(e, this, null);
        }
    }

    public void handle(MySQLClientSession mysql) throws Exception {
        ProxyBuffer proxyBuffer = mysql.currentProxyBuffer().newBufferIfNeed();
        int totalPacketEndIndex = proxyBuffer.channelReadEndIndex();
        if (!mysql.readProxyPayloadFully()) {
            return;
        }
        MySQLPayloadType payloadType = mysql.getPayloadType();
        if (!welcomePkgReceived) {
            writeClientAuth(mysql);
            return;
        }
        //收到切换登陆插件的包
        if (mysql.getPayloadType() == MySQLPayloadType.FIRST_EOF
                && mysql.getBackendPacketResolver().getState()
                == ComQueryState.AUTH_SWITCH_PLUGIN_RESPONSE) {
            //重新发送密码验证
            MySQLPacket mySQLPacket = mysql.currentProxyPayload();
            AuthSwitchRequestPacket authSwitchRequestPacket = new AuthSwitchRequestPacket();
            authSwitchRequestPacket.readPayload(mySQLPacket);

            byte[] password = generatePassword(authSwitchRequestPacket);
            mySQLPacket = mysql.newCurrentProxyPacket(1024);
            mySQLPacket.writeBytes(password);
            mysql.writeCurrentProxyPacket(mySQLPacket, mysql.getPacketId() + 1);
            mysql.getBackendPacketResolver().setIsClientLoginRequest(true);
            return;
        }
        //验证成功
        if (payloadType == MySQLPayloadType.FIRST_OK) {
            mysql.resetPacket();
            mysql.getBackendPacketResolver().setIsClientLoginRequest(false);
            callback.onFinishedOk(mysql.getBackendPacketResolver().getServerStatus(), mysql, null, null);
            return;
        }

        MySQLPacket mySQLPacket = mysql.getBackendPacketResolver().currentPayload();
        //用公钥进行密码加密
        if (STR_CACHING_AUTH_STAGE.equals(stage) && authPluginName
                .equals(CachingSha2PasswordPlugin.PROTOCOL_PLUGIN_NAME)) {
            LOGGER.info("authPluginName:{} ",authPluginName);
            String publicKeyString = mySQLPacket.readEOFString();
            byte[] payload = CachingSha2PasswordPlugin
                    .encrypt(mysqlVersion, publicKeyString, datasource.getPassword(), seed,
                            CharsetUtil.getCharset(charsetIndex));
            mySQLPacket = mysql.newCurrentProxyPacket(1024);
            mySQLPacket.writeBytes(payload);
            mysql.writeCurrentProxyPacket(mySQLPacket, mysql.getPacketId() + 1);
            mysql.getBackendPacketResolver().setIsClientLoginRequest(true);
            stage = null;
            return;
        }

        int status = mySQLPacket.getByte(4) & 0xff;
        int fastAuthResult = mySQLPacket.getByte(5) & 0xff;
        if (status == 1 && fastAuthResult == 3) {
            //验证成功继续 读取认证的ok包
            mysql.resetCurrentProxyPayload();
            proxyBuffer.channelReadEndIndex(totalPacketEndIndex);

            MySQLPacketResolver packetResolver = mysql.getBackendPacketResolver();
            mySQLPacket.packetReadStartIndex(packetResolver.getEndPos());
            mysql.getBackendPacketResolver().setIsClientLoginRequest(true);
            handle(mysql);
            return;
        }
        if (status == 1 && fastAuthResult == 4) {
            //发送payload为02的包 然后读取公钥加密密码
            byte[] payload = {(byte) 2};
            mySQLPacket = mysql.newCurrentProxyPacket(1024);
            mySQLPacket.writeBytes(payload);
            mysql.writeCurrentProxyPacket(mySQLPacket, 3);
            stage = STR_CACHING_AUTH_STAGE;
            mysql.getBackendPacketResolver().setIsClientLoginRequest(true);
            return;
        }
        //连接不上
        ErrorPacketImpl errorPacket = new ErrorPacketImpl();
        errorPacket.readPayload(mySQLPacket);
        String message = new String(errorPacket.getErrorMessage());
        LOGGER.error(message);
        mysql.resetCurrentProxyPayload();
        callback.onFinishedErrorPacket(errorPacket, mysql.getBackendPacketResolver().getServerStatus(),
                mysql, this, null);
        mysql.getBackendPacketResolver().setIsClientLoginRequest(false);
    }

    public void writeClientAuth(MySQLClientSession mysql) throws IOException {
        int serverCapabilities = GlobalConst.getClientCapabilityFlags().value;
        mysql.getBackendPacketResolver().setCapabilityFlags(serverCapabilities);
        HandshakePacket hs = new HandshakePacket();
        MySQLPacket payload = mysql.currentProxyPayload();
        if (payload.isErrorPacket()) {
            ErrorPacketImpl errorPacket = new ErrorPacketImpl();
            errorPacket.readPayload(payload);
            String errorMessage = new String(errorPacket.getErrorMessage());
            LOGGER.error(" {} {}",this.datasource.getName(),errorMessage);
            mysql.setLastMessage(errorMessage);
            onClear(mysql);
            mysql.close(false, errorMessage);
            callback.onFinishedErrorPacket(errorPacket, mysql.getBackendPacketResolver().getServerStatus(),
                    mysql, this, null);
            return;
        }

        hs.readPayload(mysql.currentProxyPayload());
        mysql.resetCurrentProxyPayload();
        this.mysqlVersion = hs.getServerVersion();
        this.charsetIndex = hs.getCharacterSet() == -1 ? CharsetUtil.getIndex("UTF-8") : hs.getCharacterSet();
        AuthPacket packet = new AuthPacket();
        packet.setCapabilities(serverCapabilities);
        packet.setMaxPacketSize(32 * 1024 * 1024);
        packet.setCharacterSet((byte) charsetIndex);
        packet.setUsername(datasource.getUsername());
        this.seed = hs.getAuthPluginDataPartOne() + hs.getAuthPluginDataPartTwo();

        LOGGER.info("backend mysql authPluginName:{} ",hs.getAuthPluginName());
        //加密密码
        this.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;//hs.getAuthPluginName();
        LOGGER.info("mycat set authPluginName:{} ",authPluginName);
        packet.setPassword(generatePassword(authPluginName, seed));
//        print(packet.getPassword());
        packet.setAuthPluginName(hs.getAuthPluginName());
//      packet.setAuthPluginName(CachingSha2PasswordPlugin.PROTOCOL_PLUGIN_NAME);
        MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(1024);
        mysql.getBackendPacketResolver().setIsClientLoginRequest(true);
        packet.writePayload(mySQLPacket);
        this.welcomePkgReceived = true;
        mysql.writeCurrentProxyPacket(mySQLPacket, 1);
    }


    /**
     * 判断插件 密码加密
     */
    public byte[] generatePassword(
            AuthSwitchRequestPacket authSwitchRequestPacket) {
        String authPluginName = authSwitchRequestPacket.getAuthPluginName();
        String authPluginData = authSwitchRequestPacket.getAuthPluginData();
        if (authPluginData != null) {
            seed = authPluginData;
        }
        return generatePassword(authPluginName, seed);
    }

    /**
     * 判断插件 密码加密
     */
    public byte[] generatePassword(
            String authPluginName, String seed) {
        LOGGER.info("authPluginName:{} ",authPluginName);
        if (MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME.equals(authPluginName)) {
            return MysqlNativePasswordPluginUtil.scramble411(datasource.getPassword(),
                    seed);
        } else if (CachingSha2PasswordPlugin.PROTOCOL_PLUGIN_NAME.equals(authPluginName)) {
            return CachingSha2PasswordPlugin.scrambleCachingSha2(datasource.getPassword(),
                    seed);
        } else {
            throw new MycatException(String.format("unknown auth plugin %s!", authPluginName));
        }
    }


    @Override
    public void onSocketWrite(MySQLClientSession session) {
        try {
            session.writeToChannel();
        } catch (Exception e) {
            onException(session, e);
            callback.onFinishedException(e, this, null);
        }
    }

    @Override
    public void onWriteFinished(MySQLClientSession mysql) {
        mysql.change2ReadOpts();
    }

    @Override
    public void onException(MySQLClientSession session, Exception e) {
        MycatMonitor.onBackendConCreateException(session, e);
        LOGGER.error("{}", e);
        onClear(session);
        session.close(false, e);
    }

    public void onClear(MySQLClientSession session) {
        session.resetPacket();
        session.setCallBack(null);
        MycatMonitor.onBackendConCreateClear(session);
    }

}
