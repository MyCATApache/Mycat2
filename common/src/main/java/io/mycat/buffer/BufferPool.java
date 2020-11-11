package io.mycat.buffer;

import io.mycat.Dumpable;

import java.nio.ByteBuffer;
import java.util.Map;

public interface BufferPool extends Dumpable {

    ByteBuffer allocate();

    ByteBuffer allocate(int size);

    ByteBuffer allocate(byte[] bytes);

    int trace();


    public default ByteBuffer expandBuffer(ByteBuffer old, int len) {
        assert old != null;
        assert len != 0 && len > old.capacity();

        int position = old.position();
        int limit = old.limit();

        ByteBuffer newBuffer = allocate(len);
        old.position(0);
        old.limit(old.capacity());
        newBuffer.put(old);
        newBuffer.position(position);
        newBuffer.limit(limit);

        recycle(old);
        return newBuffer;
    }

    void recycle(ByteBuffer theBuf);

    long capacity();

    int chunkSize();
}
