package io.mycat.proxy.task;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class FileChannelPayloadWriterHandler extends AbstractPayloadWriter<FileChannel> {
    @Override
    protected int writePayload(FileChannel fileChannel, int writeIndex, int reminsPacketLen, SocketChannel serverSocket) throws IOException {
        return (int) fileChannel.transferTo(writeIndex, reminsPacketLen, serverSocket);
    }

    @Override
    void clearResource(FileChannel f) throws Exception {
        f.close();
    }
}
