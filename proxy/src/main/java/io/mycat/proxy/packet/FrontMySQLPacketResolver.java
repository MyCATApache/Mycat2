/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.packet;

import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.session.MycatSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

/**
 * @author jamie12221 date 2019-05-20 11:52
 * mysql请求报文解析器
 **/
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
     if(-1==socketChannel.read(head)){
       throw new ClosedChannelException();
     }
      if (!head.hasRemaining()) {
        head.position(0);
        int length = MySQLPacket.readInt(head, 3);
        multiPacketList.add(payload = pool.allocate(length));
        payload.limit(length);
      } else {
        return false;
      }
    }
    if(-1==socketChannel.read(payload)){
      throw new ClosedChannelException();
    }
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

  public MySQLPacket getPayload() {
    head.position(0);
    payload = null;
    try {
      currentMySQLPacket = (ProxyBufferImpl) session.currentProxyBuffer();

      if (multiPacketList.size() == 1) {
        ByteBuffer byteBuffer = multiPacketList.remove(0);
        int size = byteBuffer.limit();
        currentMySQLPacket.reset();
        currentMySQLPacket.setBuffer(byteBuffer);
        currentMySQLPacket.channelReadStartIndex(0);
        currentMySQLPacket.channelReadEndIndex(size);
        return currentMySQLPacket;
      } else {
        int size = 0;
        for (ByteBuffer byteBuffer : multiPacketList) {
          size += byteBuffer.limit();
        }
        currentMySQLPacket.reset();
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

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    reset();
  }
}