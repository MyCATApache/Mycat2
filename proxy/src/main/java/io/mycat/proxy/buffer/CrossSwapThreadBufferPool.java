package io.mycat.proxy.buffer;

import io.mycat.MycatException;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.reactor.SessionThread;
import java.nio.ByteBuffer;

/**
 * 封装,记录buffer在跨线程的工具类 junwen12221
 */
public class CrossSwapThreadBufferPool {

  private volatile SessionThread source;
  private BufferPool bufferPool;

  public CrossSwapThreadBufferPool(
      BufferPool bufferPool) {
    this.bufferPool = bufferPool;
  }

  public ByteBuffer allocate(int size) {
    if (source != null && source != Thread.currentThread()) {
      throw new MycatException("Illegal state");
    }
    return bufferPool.allocate(size);
  }

  public ByteBuffer allocate(byte[] bytes) {
    if (source != null && source != Thread.currentThread()) {
      throw new MycatException("Illegal state");
    }
    return bufferPool.allocate(bytes);
  }

  public void recycle(ByteBuffer theBuf) {
    bufferPool.recycle(theBuf);
  }

  public void bindSource(SessionThread source) {
    this.source = source;
  }

  public SessionThread getSource() {
    return source;
  }
}