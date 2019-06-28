package io.mycat.proxy.buffer;

import io.mycat.proxy.session.Session;
import java.nio.ByteBuffer;

public interface SessionBufferPool {

  ByteBuffer allocate(Session session);

  ByteBuffer allocate(int size, Session session);

  ByteBuffer allocate(byte[] bytes, Session session);

  ByteBuffer expandBuffer(ByteBuffer buffer, int len, Session session);

  void recycle(ByteBuffer theBuf, Session session);

  long capacity();

  int chunkSize();
}