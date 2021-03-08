package io.mycat.vertx;

import io.mycat.*;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.beans.mysql.packet.AuthSwitchRequestPacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.config.UserConfig;
import io.mycat.mycatmysql.MycatMySQLHandler;
import io.mycat.mycatmysql.MycatMysqlSession;
import io.mycat.proxy.handler.front.MySQLClientAuthHandler;
import io.mycat.proxy.handler.front.SocketAddressUtil;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.util.Arrays;

import static io.mycat.beans.mysql.MySQLErrorCode.ER_ACCESS_DENIED_ERROR;
import static io.mycat.vertx.VertxMySQLPacketResolver.readInt;

public class VertxMySQLAuthHandler implements Handler<Buffer> {
    final NetSocket socket;
    final VertxMycatServer.MycatSessionManager mysqlProxyServerVerticle;
    private final MycatDataContext mycatDataContext;
    private byte[][] seedParts;
    Buffer buffer = Buffer.buffer();
    boolean authSwitchResponse = false;
    private AuthPacket authPacket;

    public VertxMySQLAuthHandler(NetSocket socket, VertxMycatServer.MycatSessionManager mysqlProxyServerVerticle) {
        this.socket = socket;
        this.mysqlProxyServerVerticle = mysqlProxyServerVerticle;
        this.mycatDataContext = new MycatDataContextImpl();
        int defaultServerCapabilities = MySQLServerCapabilityFlags.getDefaultServerCapabilities();
        this.seedParts = MysqlNativePasswordPluginUtil.nextSeedBuild();
        byte[] handshakePacket = MySQLClientAuthHandler.createHandshakePayload(mycatDataContext.getSessionId(), defaultServerCapabilities, seedParts);
        socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(0, handshakePacket)));
        socket.handler(this);
    }

    @Override
    public void handle(Buffer event) {
        buffer.appendBuffer(event);
        if (buffer.length() > 3) {
            int length = readInt(buffer, 0, 3);
            if (length == buffer.length() - 4) {
                int packetId = buffer.getUnsignedByte(3);
                Buffer payload = buffer.slice(4, buffer.length());
                ReadView readView = new ReadView(payload);
                if (!authSwitchResponse) {
                    this.authPacket = new AuthPacket();
                    authPacket.readPayload(readView);
                    if ("mysql_native_password".equalsIgnoreCase(authPacket.getAuthPluginName())
                            ||
                            authPacket.getAuthPluginName() == null) {
                        auth(packetId);
                    } else {
                        authSwitchResponse = true;
                        buffer = Buffer.buffer();

                        AuthSwitchRequestPacket authSwitchRequestPacket = new AuthSwitchRequestPacket();
                        authSwitchRequestPacket.setStatus((byte) 0xfe);
                        authSwitchRequestPacket.setAuthPluginName("mysql_native_password");
                        authSwitchRequestPacket.setAuthPluginData(new String(seedParts[2]));
                        MySQLPayloadWriter mySQLPayloadWriter = new MySQLPayloadWriter(1024);
                        authSwitchRequestPacket.writePayload(mySQLPayloadWriter);

                        socket.write(Buffer.buffer(
                                MySQLPacketUtil.generateMySQLPacket(packetId + 1, mySQLPayloadWriter)));
                        return;
                    }
                } else {
                    byte[] bytes = readView.readEOFStringBytes();
                    authPacket.setPassword(bytes);
                    auth(packetId);
                }

            }
        }
    }

    private void auth(int packetId) {
        String username = authPacket.getUsername();
        String host = SocketAddressUtil.simplySocketAddress(socket.remoteAddress().toString());
        Authenticator authenticator = null;
        if (MetaClusterCurrent.exist(Authenticator.class)) {
            authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        }
        if (authenticator != null) {
            Authenticator.AuthInfo authInfo = authenticator.getPassword(username,
                    host);
            String rightPassword = (
                    authInfo.getRightPassword());
            if (!checkPassword(rightPassword, authPacket.getPassword())) {
                String message = "Access denied for user '" +
                        username +
                        "'@'" +
                        host +
                        "' (using password: YES)";
                socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(packetId+1,
                        MySQLPacketUtil.generateError(ER_ACCESS_DENIED_ERROR, message, 0))));
                socket.end();
                return;
            }
        }
        buffer = null;
        UserConfig userInfo = null;
        if (authenticator != null) {
            userInfo = authenticator.getUserInfo(username);
        }

        mycatDataContext.setUser(new MycatUser(username, null, null, host, userInfo));
        mycatDataContext.useShcema(authPacket.getDatabase());
        mycatDataContext.setServerCapabilities(authPacket.getCapabilities());
        mycatDataContext.setAutoCommit(true);
        mycatDataContext.setIsolation(MySQLIsolation.REPEATED_READ);
        mycatDataContext.setCharsetIndex(authPacket.getCharacterSet());
        MycatMysqlSession vertxSession = new MycatMysqlSession(mycatDataContext, socket);
        socket.handler(new VertxMySQLPacketResolver(socket, new MycatMySQLHandler(vertxSession)));
        vertxSession.setPacketId(packetId);

        mysqlProxyServerVerticle.addSession(vertxSession);

        vertxSession.writeOkEndPacket();
    }

    private boolean checkPassword(String rightPassword, byte[] password) {
        if (rightPassword == null || rightPassword.length() == 0) {
            return (password == null || password.length == 0);
        }
        if (password == null || password.length == 0) {
            return false;
        }
        byte[] encryptPass = MysqlNativePasswordPluginUtil.scramble411(rightPassword, seedParts[2]);
        return Arrays.equals(password, encryptPass);
    }


}
