package io.mycat.proxy.buffer;

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.Session;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author jamie12221
 * date 2019-05-16 10:09
 **/
public final class ProxyBufferPoolMonitor implements BufferPool {
    final BufferPool bufferPool;

    public ProxyBufferPoolMonitor(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    @Override
    public void init(Map<String, String> args) {

    }

    @Override
    public ByteBuffer allocate() {
        return bufferPool.allocate();
    }

    @Override
    public ByteBuffer allocate(int size) {
        ByteBuffer allocate = bufferPool.allocate(size);
        MycatMonitor.onAllocateByteBuffer(allocate, getSession());
        return allocate;
    }

    public Session getSession() {
        SessionThread thread = (SessionThread) Thread.currentThread();
        return thread.getCurSession();
    }

    @Override
    public ByteBuffer allocate(byte[] bytes) {
        return bufferPool.allocate(bytes);
    }

    @Override
    public ByteBuffer expandBuffer(ByteBuffer old, int len) {
        int chunkSize = bufferPool.chunkSize();
        ByteBuffer byteBuffer = bufferPool.expandBuffer(old, (len / chunkSize + 1) * chunkSize);
        MycatMonitor.onExpandByteBuffer(byteBuffer, getSession());
        return byteBuffer;
    }

    @Override
    public void recycle(ByteBuffer theBuf) {
        MycatMonitor.onRecycleByteBuffer(theBuf, getSession());
        bufferPool.recycle(theBuf);
    }

    @Override
    public long capacity() {
        return bufferPool.capacity();
    }

    @Override
    public int chunkSize() {
        return bufferPool.chunkSize();
    }
}
