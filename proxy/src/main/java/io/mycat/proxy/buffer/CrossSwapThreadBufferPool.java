package io.mycat.proxy.buffer;

import io.mycat.MycatException;
import io.mycat.buffer.BufferPool;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;

import java.nio.ByteBuffer;

/**
 * 封装,记录buffer在跨线程的工具类 junwen12221
 */
public class CrossSwapThreadBufferPool {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(CrossSwapThreadBufferPool.class);
  private volatile SessionThread source;
  private BufferPool bufferPool;

  public CrossSwapThreadBufferPool(
      BufferPool bufferPool) {
    this.bufferPool = bufferPool;
  }

  public ByteBuffer allocate(int size) {
    check();
    return bufferPool.allocate(size);
  }

  public ByteBuffer allocate(byte[] bytes) {
    check();
    return bufferPool.allocate(bytes);
  }

  private void check() {
    if (source != null && source != Thread.currentThread()) {
      LOGGER.error("{}", Thread.currentThread());
      throw new MycatException("Illegal state");
    }
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