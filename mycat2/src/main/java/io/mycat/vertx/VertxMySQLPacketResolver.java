package io.mycat.vertx;

import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.text.MessageFormat;

import static io.mycat.vertx.VertxMySQLPacketResolver.State.HEAD;
import static io.mycat.vertx.VertxMySQLPacketResolver.State.PAYLOAD;

public class VertxMySQLPacketResolver implements Handler<Buffer> {
    final VertxMySQLHandler mySQLHandler;
    Buffer head;
    Buffer payload;
    int mycatPacketCounter = 0;
    State state = HEAD;
    int currentPacketLength;
    NetSocket socket;
    volatile Buffer queue;
    private short packetId;
    private volatile boolean running = false;

    static enum State {
        HEAD,
        PAYLOAD
    }


    public VertxMySQLPacketResolver(NetSocket socket, VertxMySQLHandler mySQLHandler) {
        this.mySQLHandler = mySQLHandler;
        this.socket = socket;
    }

    @Override
    public synchronized void handle(Buffer event) {
        if (running) {
            if (queue != null) {
                queue = queue.appendBuffer(event);
            } else {
                queue = event;
            }
        }
        for (; ; ) {
            if (queue != null) {
                event = queue.appendBuffer(event);
                queue = null;
            }
            switch (state) {
                case HEAD: {
                    if (head == null) {
                        head = Buffer.buffer();
                    }
                    int restEnd = 4 - head.length();
                    head.appendBuffer(event.slice(0, Math.min(event.length(), restEnd)));
                    if (head.length() < 4) {
                        state = HEAD;
                        return;
                    } else {
                        state = PAYLOAD;
                        event = event.slice(restEnd, event.length());
                    }
                    if (state == PAYLOAD) {
                        currentPacketLength = readInt(head, 0, 3);
                        this.packetId = head.getUnsignedByte(3);
                        if (mycatPacketCounter != packetId) {
                            throw new AssertionError(MessageFormat.format("packet id not match " +
                                    "mycat:{0} ordinal packet:{1}", mycatPacketCounter, packetId));
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
                    int rest = currentPacketLength - payload.length();
                    if (event.length() <= rest) {
                        payload =  payload.appendBuffer(event);
                    } else {
                        Buffer slice = event.slice(0, rest);
                        payload = payload.appendBuffer(slice);
                        queue = (event.slice(rest, event.length()));
                    }
                    event = Buffer.buffer(0);

                    boolean multiPacket = (currentPacketLength == MySQLPacketSplitter.MAX_PACKET_SIZE);
                    if (multiPacket) {
                        if ((MySQLPacketSplitter.MAX_PACKET_SIZE) == payload.length()) {
                            state = HEAD;
                            continue;
                        }
                        return;
                    } else {
                        if (payload.length() == currentPacketLength) {
                            Buffer payload = this.payload;
                            this.payload = null;
                            state = HEAD;
                            mycatPacketCounter = 0;

                            Context context = Vertx.currentContext();
                            running = true;
                            context.executeBlocking(event1 -> {
                                mySQLHandler.handle(packetId, payload, socket);
                            });
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
        for (int i = 0; i < length; i++) {
            byte b = buffer.getByte(start + i);
            rv |= (((long) b) & 0xFF) << (i * 8);
        }
        return rv;
    }

    public synchronized void nextPacket() {
        running = false;
        if (queue != null) {
            Buffer buffer = this.queue;
            this.queue = null;
            handle(buffer);
        }

    }
}
