package io.mycat.proxy.buffer;

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.Session;
import java.nio.ByteBuffer;

public class SessionBufferPoolImpl implements SessionBufferPool {

  final BufferPool bufferPool;

  public SessionBufferPoolImpl(BufferPool bufferPool) {
    this.bufferPool = bufferPool;
  }

  @Override
  public ByteBuffer allocate(Session session) {
    ByteBuffer byteBuffer = this.bufferPool.allocate();
    MycatMonitor.onAllocateByteBuffer(byteBuffer, session);
    return byteBuffer;
  }

  @Override
  public ByteBuffer allocate(int size, Session session) {
    ByteBuffer byteBuffer = this.bufferPool.allocate(size);
    MycatMonitor.onAllocateByteBuffer(byteBuffer, session);
    return byteBuffer;
  }

  @Override
  public ByteBuffer allocate(byte[] bytes, Session session) {
    ByteBuffer byteBuffer = this.bufferPool.allocate(bytes);
    MycatMonitor.onAllocateByteBuffer(byteBuffer, session);
    return byteBuffer;
  }

  @Override
  public ByteBuffer expandBuffer(ByteBuffer buffer, int len, Session session) {
    ByteBuffer byteBuffer = this.bufferPool.expandBuffer(buffer, len);
    MycatMonitor.onExpandByteBuffer(byteBuffer, session);
    return byteBuffer;
  }

  @Override
  public void recycle(ByteBuffer theBuf, Session session) {
    this.bufferPool.recycle(theBuf);
    MycatMonitor.onRecycleByteBuffer(theBuf, session);
  }

  @Override
  public long capacity() {
    return this.bufferPool.capacity();
  }

  @Override
  public long size() {
    return this.bufferPool.size();
  }

  @Override
  public int getChunkSize() {
    return this.bufferPool.getChunkSize();
  }
}