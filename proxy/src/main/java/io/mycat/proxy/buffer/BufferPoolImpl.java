package io.mycat.proxy.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class BufferPoolImpl implements BufferPool {
    @Override
    public ByteBuffer allocate() {
        return ByteBuffer.allocate(128);
    }

    @Override
    public ByteBuffer allocate(int size) {
        return  ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer expandBuffer(ByteBuffer buffer) {
        return expandBuffer(buffer,buffer.capacity()<<1);
    }

    @Override
    public ByteBuffer expandBuffer(ByteBuffer buffer, int len) {
        int position = buffer.position();
        int limit = buffer.limit();

        ByteBuffer allocate = ByteBuffer.allocate(len);
        buffer.position(0);
        buffer.limit(buffer.capacity());
        allocate.put(buffer);
        allocate.position(position);
        allocate.limit(limit);
        return allocate;


    }

    @Override
    public void recycle(ByteBuffer theBuf) {
        theBuf.clear();
    }

    @Override
    public long capacity() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public int getSharedOptsCount() {
        return 0;
    }

    @Override
    public int getChunkSize() {
        return 0;
    }

    @Override
    public ConcurrentHashMap<Long, Long> getNetDirectMemoryUsage() {
        return null;
    }
}
