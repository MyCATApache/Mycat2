package io.mycat;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

public class VertxMySQLPacketResolver implements Handler<Buffer> {
    Buffer head = Buffer.buffer();
    Buffer payload = Buffer.buffer();
    int packetCounter = 0;
    MysqlProxyServer.State state = MysqlProxyServer.State.HEAD;
    int currentPacketLength;
    VertxMySQLHandler mySQLHandler;
    NetSocket socket;
    private int packetId;

    public VertxMySQLPacketResolver(NetSocket socket, VertxMySQLHandler mySQLHandler) {
        this.mySQLHandler = mySQLHandler;
        this.socket = socket;
    }

    @Override
    public void handle(Buffer event) {
        for (; ; ) {
            switch (state) {
                case HEAD: {
                    if (head == null) {
                        head = Buffer.buffer();
                    }
                    if (head.length() < 4) {
                        if (event.length() >= 4) {
                            head.appendBuffer(event.slice(0, 4));
                            state = MysqlProxyServer.State.PAYLOAD;
                            event = event.slice(4, event.length());
                        } else {
                            head.appendBuffer(event);
                            state = MysqlProxyServer.State.HEAD;
                            return;
                        }
                    } else {
                        state = MysqlProxyServer.State.PAYLOAD;
                    }
                    if (state == MysqlProxyServer.State.PAYLOAD) {
                        currentPacketLength = readInt(head, 0, 3);
                       this. packetId = head.getUnsignedByte(3);
                        assert packetCounter == packetId;
                        ++packetCounter;
                        head = null;//help gc
                    }
                    continue;
                }
                case PAYLOAD: {
                    if (payload == null) {
                        payload = Buffer.buffer();
                    }
                    payload.appendBuffer(event);
                    boolean multiPacket = (currentPacketLength == (2 ^ 24 - 1));
                    if (multiPacket) {
                        if (payload.length() == (2 ^ 24)) {
                            state = MysqlProxyServer.State.HEAD;
                        }
                        return;
                    } else {
                        if (payload.length() == currentPacketLength) {
                            Buffer payload = this.payload;
                            this.payload = null;
                            mySQLHandler.handle(packetId,payload, socket);
                            return;
                        }
                    }
                    return;
                }

            }
        }
    }

    public static int readInt(Buffer buffer, int start, int length) {
        int rv = 0;
        for (int i = start; i < length; i++) {
            byte b = buffer.getByte(i);
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }
}
