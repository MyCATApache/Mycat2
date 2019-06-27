package io.mycat.buffer;

import io.mycat.MycatExpection;
import java.nio.ByteBuffer;

public class CrossSwapThreadBufferPool {
  private volatile Thread source;
  private final Thread target;
  private BufferPool bufferPool;

  public CrossSwapThreadBufferPool(Thread target,
      BufferPool bufferPool) {
    this.target = target;
    this.bufferPool = bufferPool;
  }

  public ByteBuffer allocate(int size) {
    if (source != null && source != Thread.currentThread()) {
      throw new MycatExpection("Illegal state");
    }
    return bufferPool.allocate(size);
  }

  public ByteBuffer allocate(byte[] bytes) {
    if (source != null && source != Thread.currentThread()) {
      throw new MycatExpection("Illegal state");
    }
    return bufferPool.allocate(bytes);
  }

  public void recycle(ByteBuffer theBuf) {
    if (Thread.currentThread()== source) {
      throw new MycatExpection("Illegal state");
    }
    bufferPool.recycle(theBuf);
  }

  public void bindSource(Thread source) {
    if (this.source ==null) {
      this.source = source;
    } else {
      throw new MycatExpection("unsupport operation");
    }
  }

  public void unbindSource(Thread source) {
    if (this.source == source) {
      this.source = null;
    } else {
      throw new MycatExpection("unsupport operation");
    }
  }
}