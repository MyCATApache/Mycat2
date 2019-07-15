package io.mycat.proxy.buffer;

import io.mycat.MycatException;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MycatSession;
import java.nio.ByteBuffer;

/**
 * 封装,记录buffer在跨线程的工具类 junwen12221
 */
public class CrossSwapThreadBufferPool {

  private volatile ReactorEnvThread source;
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
    Thread thread = Thread.currentThread();
    if (source != null && source != thread) {
      System.out.println();
    }
    return bufferPool.allocate(bytes);
  }

  public void recycle(ByteBuffer theBuf) {
    bufferPool.recycle(theBuf);
  }

  public void bindSource(ReactorEnvThread source) {
    System.out.println("----------------------------------------"+source);
    this.source = source;
  }
//
//  public void unbindSource(ReactorEnvThread source) {
////    if (this.source == source) {
////      this.source = null;
////    } else {
////      throw new MycatException("unsupport operation");
////    }
//  }

  public ReactorEnvThread getSource() {
    return source;
  }
}