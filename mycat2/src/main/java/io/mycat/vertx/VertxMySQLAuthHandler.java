package io.mycat.vertx;

import io.mycat.Authenticator;
import io.mycat.MetaClusterCurrent;
import io.mycat.MySQLPacketUtil;
import io.mycat.MycatUser;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.config.UserConfig;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.proxy.handler.front.MySQLClientAuthHandler;
import io.mycat.proxy.handler.front.SocketAddressUtil;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static io.mycat.vertx.VertxMySQLPacketResolver.readInt;
import static io.mycat.beans.mysql.MySQLErrorCode.ER_ACCESS_DENIED_ERROR;

public class VertxMySQLAuthHandler implements Handler<Buffer> {
    final NetSocket socket;
    final VertxMycatServer.MycatSessionManager mysqlProxyServerVerticle;
    private final MycatDataContextImpl mycatDataContext;
    private byte[][] seedParts;
    Buffer buffer = Buffer.buffer();

    public VertxMySQLAuthHandler(NetSocket socket, VertxMycatServer.MycatSessionManager mysqlProxyServerVerticle) {
        this.socket = socket;
        this.mysqlProxyServerVerticle = mysqlProxyServerVerticle;
        this. mycatDataContext = new MycatDataContextImpl();
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
                AuthPacket authPacket = new AuthPacket();
                authPacket.readPayload(readView);
                if ("mysql_native_password".equalsIgnoreCase(authPacket.getAuthPluginName())
                        ||
                        authPacket.getAuthPluginName()==null) {
                    String username = authPacket.getUsername();
                    String host =  SocketAddressUtil.simplySocketAddress(socket.remoteAddress().toString());
                    Authenticator authenticator = null;
                    if (MetaClusterCurrent.exist(Authenticator.class)) {
                        authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
                    }
                    if (authenticator != null) {
                        Authenticator.AuthInfo authInfo = authenticator.getPassword(username,
                                host);
                        String rightPassword = Objects.requireNonNull(
                                authInfo.getRightPassword());
                        if (!checkPassword(rightPassword, authPacket.getPassword())) {
                            String message = "Access denied for user '" +
                                    username +
                                    "'@'" +
                                    host +
                                    "' (using password: YES)";
                            socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(2,
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
                    VertxSession vertxSession = new VertxSessionImpl(mycatDataContext, socket);
                    mycatDataContext.useShcema(authPacket.getDatabase());
                    mycatDataContext.setServerCapabilities(authPacket.getCapabilities());
                    mycatDataContext.setAutoCommit(true);
                    mycatDataContext.setIsolation(MySQLIsolation.READ_UNCOMMITTED);
                    mycatDataContext.setCharsetIndex(authPacket.getCharacterSet());
                    JdbcConnectionManager connection = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    connection.getDatasourceProvider().createSession(mycatDataContext);
                    socket.handler(new VertxMySQLPacketResolver(socket, new VertxMySQLHandler(vertxSession)));
                    vertxSession.setPacketId(packetId);

                    mysqlProxyServerVerticle.addSession(vertxSession);

                    vertxSession.writeOkEndPacket();
                }else {
                    socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(
                            packetId+1,
                            MySQLPacketUtil.generateError(MySQLErrorCode.ER_UNKNOWN_ERROR,"need mysql_native_password plugin",0)
                    )));
                    return;
                }
            }
        }
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
