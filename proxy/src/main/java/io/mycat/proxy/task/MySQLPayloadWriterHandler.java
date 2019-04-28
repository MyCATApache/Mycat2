package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class MySQLPayloadWriterHandler extends AbstractPayloadWriter<MySQLPacket> {
    @Override
    protected int writePayload(MySQLPacket buffer, int writeIndex, int reminsPacketLen, SocketChannel serverSocket) throws IOException {
        return serverSocket.write(buffer.currentBuffer().currentByteBuffer());
    }

    @Override
    void clearResource(MySQLPacket f) throws Exception {
        f.reset();
    }
}
