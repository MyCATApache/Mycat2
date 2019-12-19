package io.mycat.proxy.buffer;

import io.mycat.buffer.Mycat16BufferPoolImpl;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.Session;
import java.nio.ByteBuffer;

/**
 * @author jamie12221
 *  date 2019-05-16 10:09
 **/
public final class ProxyBufferPoolMonitor extends Mycat16BufferPoolImpl {

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
    SessionThread thread = (SessionThread)Thread.currentThread();
    return thread.getCurSession();
  }

  @Override
  public ByteBuffer allocate(byte[] bytes) {
    return super.allocate(bytes);
  }

  @Override
  public ByteBuffer expandBuffer(ByteBuffer old, int len) {
    int chunkSize = chunkSize();
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
