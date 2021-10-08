package io.mycat.swapbuffer;

import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;

public class PacketRequestResponse extends PacketRequestImpl implements PacketResponse{
    int copyCount;
    public PacketRequestResponse(ByteBuffer body, int offset, int length) {
        super(body, offset, length);
    }


    @Override
    public PacketRequest getRequest() {
        return this;
    }

    @Override
    public int getCopyCount() {
        return copyCount;
    }

    @Override
    public void setCopyCount(int n) {
         copyCount = n;
    }
}
