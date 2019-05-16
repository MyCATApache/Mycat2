package io.mycat.proxy.buffer;

import io.mycat.buffer.BufferPoolImpl;
import io.mycat.proxy.MycatMonitor;
import java.nio.ByteBuffer;

/**
 * @author jamie12221
 * @date 2019-05-16 10:09
 **/
public class MycatProxyBufferPoolImpl extends BufferPoolImpl {

  public MycatProxyBufferPoolImpl(int pageSize, int chunkSize, int pageCount) {
    super(pageSize, chunkSize, pageCount);
  }

  @Override
  public ByteBuffer allocate(int size) {
    ByteBuffer allocate = super.allocate(size);
    MycatMonitor.onAllocateByteBuffer(allocate);
    return allocate;
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
    MycatMonitor.onExpandByteBuffer(byteBuffer);
    return byteBuffer;
  }

  @Override
  public void recycle(ByteBuffer theBuf) {
    MycatMonitor.onRecycleByteBuffer(theBuf);
    super.recycle(theBuf);
  }
}
