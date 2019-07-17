package io.mycat.proxy.packet;

import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

public class FrontMySQLPacketResolver {

  final ByteBuffer head = ByteBuffer.allocate(4);
  ByteBuffer payload = null;
  final ArrayList<ByteBuffer> multiPacketList = new ArrayList<>();
  final BufferPool pool;
  final MycatSession session;
  ProxyBufferImpl currentMySQLPacket = null;

  public FrontMySQLPacketResolver(BufferPool pool, MycatSession session) {
    this.pool = pool;
    this.session = session;
  }

  public void setPacketId(int packetId) {
    session.setPacketId(packetId);
  }

  public boolean readFromChannel() throws IOException {
    SocketChannel socketChannel = session.channel();
    if (head.position() == 0) {
      socketChannel.read(head);
      if (!head.hasRemaining()) {
        head.position(0);
        int length = MySQLPacket.readInt(head, 3);
        multiPacketList.add(payload = pool.allocate(length));
        payload.limit(length);
      } else {
        return false;
      }
    }
    socketChannel.read(payload);
    if (payload.hasRemaining()) {
      return false;
    } else {
      boolean multiPacket = payload.position() == MySQLPacketSplitter.MAX_PACKET_SIZE;
      if (!multiPacket) {
        setPacketId(head.get(3));
        return true;
      } else {
        head.position(0);
        payload = null;
        return readFromChannel();
      }
    }
  }

  private void clearQueue() {
    int size = multiPacketList.size();
    for (int i = size - 1; i >= 0; i--) {
      pool.recycle(multiPacketList.remove(i));
    }
    if (payload != null) {
      pool.recycle(payload);
    }
  }

  public void reset() {
    clearQueue();
    if (currentMySQLPacket != null) {
      currentMySQLPacket.reset();
    }
  }

  public MySQLPacket getPayload(BufferPool bufferPool) {
    assert !head.hasRemaining();
    head.position(0);
    payload = null;
    try {
      currentMySQLPacket = (ProxyBufferImpl) session.currentProxyBuffer();
      if (multiPacketList.size() == 1) {
        ByteBuffer byteBuffer = multiPacketList.remove(0);
        int size = byteBuffer.limit();
        currentMySQLPacket.setBuffer(byteBuffer, bufferPool);
        currentMySQLPacket.channelReadStartIndex(0);
        currentMySQLPacket.channelReadEndIndex(size);
        return currentMySQLPacket;
      } else {
        int size = 0;
        for (ByteBuffer byteBuffer : multiPacketList) {
          size += byteBuffer.limit();
        }
        currentMySQLPacket.newBuffer(size);
        for (ByteBuffer byteBuffer : multiPacketList) {
          byteBuffer.position(0);
          currentMySQLPacket.put(byteBuffer);
        }
        currentMySQLPacket.channelReadStartIndex(0);
        currentMySQLPacket.channelReadEndIndex(size);
        return currentMySQLPacket;
      }
    } finally {
      clearQueue();
    }
  }

  public MySQLPacket getPayload() {
    return getPayload(pool);
  }

}