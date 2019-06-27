package io.mycat.proxy.buffer;

import io.mycat.buffer.BufferPoolImpl;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.Session;
import java.nio.ByteBuffer;

/**
 * @author jamie12221
 *  date 2019-05-16 10:09
 **/
public final class ProxyBufferPoolMonitor extends BufferPoolImpl {

  public ProxyBufferPoolMonitor(int pageSize, int chunkSize, int pageCount) {
    super(pageSize, chunkSize, pageCount);
  }

  @Override
  public ByteBuffer allocate(int size) {
    ByteBuffer allocate = super.allocate(size);
    MycatMonitor.onAllocateByteBuffer(allocate, getSession());
    return allocate;
  }

  public Session getSession() {
    ReactorEnvThread thread = (ReactorEnvThread)Thread.currentThread();
    return thread.getReactorEnv().getCurSession();
  }

  @Override
  public ByteBuffer allocate(byte[] bytes) {
    ByteBuffer allocate = super.allocate(bytes);
    return allocate;
  }

  @Override
  public ByteBuffer expandBuffer(ByteBuffer old, int len) {
    int chunkSize = getChunkSize();
    ByteBuffer byteBuffer = super.expandBuffer(old, (len / chunkSize + 1) * chunkSize);
    MycatMonitor.onExpandByteBuffer(byteBuffer,getSession());
    return byteBuffer;
  }

  @Override
  public void recycle(ByteBuffer theBuf) {
    MycatMonitor.onRecycleByteBuffer(theBuf,getSession());
    super.recycle(theBuf);
  }
}
