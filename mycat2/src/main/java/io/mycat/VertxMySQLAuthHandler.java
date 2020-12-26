package io.mycat;

import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.handler.front.MySQLClientAuthHandler;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static io.mycat.VertxMySQLPacketResolver.readInt;
import static io.mycat.beans.mysql.MySQLErrorCode.ER_ACCESS_DENIED_ERROR;

public class VertxMySQLAuthHandler implements Handler<Buffer> {
    final NetSocket socket;
    private Authenticator authenticator;
    final VertxMySQLHandler mySQLHandler;
    public static final AtomicInteger sessionId = new AtomicInteger();
    private byte[][] seedParts;
    Buffer buffer = Buffer.buffer();

    public VertxMySQLAuthHandler(NetSocket socket, Authenticator authenticator, VertxMySQLHandler mySQLHandler) {
        this.socket = socket;
        this.authenticator = authenticator;
        this.mySQLHandler = mySQLHandler;
        int id = sessionId.getAndIncrement();
        int defaultServerCapabilities = MySQLServerCapabilityFlags.getDefaultServerCapabilities();
        this.seedParts = MysqlNativePasswordPluginUtil.nextSeedBuild();
        byte[] handshakePacket = MySQLClientAuthHandler.createHandshakePayload(id, defaultServerCapabilities, seedParts);
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
                Buffer payload = buffer.slice(4, 4 + buffer.length());
                ReadView readView = new ReadView(payload);
                AuthPacket authPacket = new AuthPacket();
                authPacket.readPayload(readView);
                if ("mysql_native_password".equals(authPacket.getAuthPluginName())) {
                    String username = authPacket.getUsername();
                    String host = socket.remoteAddress().host();
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
                    socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(2,
                            MySQLPacketUtil.generateOk(0, 0, 0, 0, 0, false, false, false, ""))));
                    socket.handler(new VertxMySQLPacketResolver(socket,mySQLHandler));
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
