package io.mycat.buffer;

import io.mycat.util.Dumper;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DirectByteBufferPool implements BufferPool {
    private DirectByte16BufferPool directByte16BufferPool = null;
    private final AtomicInteger trace = new AtomicInteger(0);

    @Override
    public void init(Map<String, String> args) {
        int pageSize = Integer.parseInt(args.getOrDefault("pageSize", 1024 * 1024 + ""));
        int chunkSize = Integer.parseInt(args.getOrDefault("chunkSize", "512"));
        int pageCount = Integer.parseInt(args.getOrDefault("pageCount", "64"));
        this.directByte16BufferPool = new DirectByte16BufferPool(pageSize, chunkSize, pageCount);
    }

    @Override
    public ByteBuffer allocate() {
        trace.getAndIncrement();
        return this.directByte16BufferPool.allocate();
    }

    @Override
    public ByteBuffer allocate(int size) {
        trace.getAndIncrement();
        return this.directByte16BufferPool.allocate(size);
    }

    @Override
    public ByteBuffer allocate(byte[] bytes) {
        trace.getAndIncrement();
        ByteBuffer allocate = this.directByte16BufferPool.allocate(bytes.length);
        allocate.put(bytes);
        allocate.position(0);
        return allocate;
    }

    @Override
    public int trace() {
        return trace.get();
    }

    @Override
    public void recycle(ByteBuffer theBuf) {
        trace.decrementAndGet();
        this.directByte16BufferPool.recycle(theBuf);
    }

    @Override
    public long capacity() {
        return this.directByte16BufferPool.capacity();
    }

    @Override
    public int chunkSize() {
        return this.directByte16BufferPool.getChunkSize();
    }

    @Override
    public Dumper snapshot() {
        return Dumper.create()
                .addText("trace",this.trace())
                .addText("chunkSize",this.chunkSize());
    }
}