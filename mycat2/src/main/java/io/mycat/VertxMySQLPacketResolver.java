package io.mycat;

import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.text.MessageFormat;

import static io.mycat.VertxMySQLPacketResolver.State.HEAD;
import static io.mycat.VertxMySQLPacketResolver.State.PAYLOAD;

public class VertxMySQLPacketResolver implements Handler<Buffer> {
    Buffer head ;
    Buffer payload ;
    int mycatPacketCounter = 0;
    State state = HEAD;
    int currentPacketLength;
    VertxMySQLHandler mySQLHandler;
    NetSocket socket;
    private int reveicePayloadCount = 0;
    static enum State {
        HEAD,
        PAYLOAD
    }


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
                    int restEnd = 4 - head.length();
                    head.appendBuffer(event.slice(0, Math.min(event.length(),restEnd)));
                    if (head.length() < 4) {
                        state =HEAD;
                        return;
                    } else {
                        state =PAYLOAD;
                        event = event.slice(restEnd, event.length());
                    }
                    if (state ==PAYLOAD) {
                        currentPacketLength = readInt(head, 0, 3);
                      int packetId = head.getUnsignedByte(3);
                        if (mycatPacketCounter != packetId){
                            throw new AssertionError(MessageFormat.format("packet id not match " +
                                    "mycat:{0} ordinal packet:{1}", mycatPacketCounter,packetId));
                        }
                        ++mycatPacketCounter;
                        head = null;//help gc
                    }
                    continue;
                }
                case PAYLOAD: {
                    if (payload == null) {
                        payload = Buffer.buffer();
                    }
                    payload.appendBuffer(event);
                    reveicePayloadCount+=event.length();
                    boolean multiPacket = (currentPacketLength == MySQLPacketSplitter.MAX_PACKET_SIZE);
                    if (multiPacket) {
                        if ((MySQLPacketSplitter.MAX_PACKET_SIZE) == reveicePayloadCount) {
                            state =HEAD;
                        }
                        return;
                    } else {
                        if (reveicePayloadCount  == currentPacketLength) {
                            Buffer payload = this.payload;
                            this.payload = null;
                            reveicePayloadCount = 0;
                            state =HEAD;
                            mycatPacketCounter = 0;
                            mySQLHandler.handle(reveicePayloadCount, payload, socket);
                            return;
                        }
                    }
                    return;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + state);
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
