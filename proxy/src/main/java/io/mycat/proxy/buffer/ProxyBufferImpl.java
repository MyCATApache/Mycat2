/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.buffer;

import io.mycat.MycatExpection;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.util.DumpUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

public final class ProxyBufferImpl implements ProxyBuffer, MySQLPacket<ProxyBufferImpl> {
  ByteBuffer buffer;
  BufferPool bufferPool;
  int readStartIndex;
  int readEndIndex;


  public ProxyBufferImpl(BufferPool bufferPool) {
    this.bufferPool = bufferPool;
  }

  @Override
  public ByteBuffer currentByteBuffer() {
    return buffer;
  }

  @Override
  public int capacity() {
    return buffer.capacity();
  }

  @Override
  public int position() {
    return buffer.position();
  }

  @Override
  public int position(int index) {
    buffer.position(index);
    return index;
  }

  @Override
  public void writeFloat(float f) {
    buffer.putFloat(f);
  }

  @Override
  public float readFloat() {
    return buffer.getFloat(readStartIndex);
  }

  @Override
  public MySQLPacket writeLong(long l) {
    buffer.putLong(l);
    return this;
  }

  @Override
  public int length() {
    return readEndIndex - readStartIndex;
  }

  @Override
  public long readLong() {
    return buffer.getLong(readStartIndex);
  }

  @Override
  public MySQLPacket writeDouble(double d) {
    buffer.putDouble(d);
    return this;
  }

  @Override
  public double readDouble() {
    return buffer.getDouble(readStartIndex);
  }

  @Override
  public void writeShort(short o) {
    buffer.putShort(o);
  }


  @Override
  public byte get() {
    return buffer.get();
  }



  @Override
  public byte get(int index) {
    return buffer.get(index);
  }

  @Override
  public ProxyBuffer get(byte[] bytes) {
    buffer.get(bytes);
    return this;
  }

  @Override
  public byte put(byte b) {
    buffer.put(b);
    return b;
  }

  @Override
  public void put(byte[] bytes, int offset, int legnth) {
    buffer.put(bytes, offset, legnth);
  }
  public void put(byte[] bytes) {
    buffer.put(bytes);
  }
  @Override
  public int channelWriteStartIndex() {
    return buffer.position();
  }

  @Override
  public int channelWriteEndIndex() {
    return buffer.limit();
  }

  @Override
  public int channelReadStartIndex() {
    return readStartIndex;
  }

  @Override
  public int channelReadEndIndex() {
    return readEndIndex;
  }

  @Override
  public void channelWriteStartIndex(int index) {
    buffer.position(index);
  }

  @Override
  public void channelWriteEndIndex(int index) {
    buffer.limit(index);
  }

  @Override
  public void channelReadStartIndex(int index) {
    readStartIndex = index;
  }

  @Override
  public void channelReadEndIndex(int index) {
    readEndIndex = index;
  }

  @Override
  public boolean readFromChannel(SocketChannel channel) throws IOException {
    try {
      buffer.limit(buffer.capacity());
      int readEndIndex = this.readEndIndex;
      buffer.position(readEndIndex);

      int readed = channel.read(buffer);
      if (readed == -1) {
        logger.warn("Read EOF ,socket closed ");
        throw new ClosedChannelException();
      } else if (readed == 0) {
        throw new MycatExpection("readed zero bytes ,Maybe a bug ,please fix it !!!!");
      }
      this.channelReadEndIndex(buffer.position());
      System.out.println("rec:");
      System.out.println(
          DumpUtil.dumpAsHex(buffer, 0,      buffer.position()));
      return readed > 0;
    } catch (Exception e) {
      throw e;
    }
  }
//
//    ByteBuffer header = ByteBuffer.allocate(4);
//    public boolean writeToChannel2(SocketChannel channel) throws IOException {
//        ByteBuffer[] bufferList = null;
//        channel.write(bufferList)
//    }
//    public boolean readFromChannel2(SocketChannel channel) throws IOException {
//        ByteBuffer[] bufferList = new ArrayList<>();
//            while (true){
//                if (header.hasRemaining()) {
//                    channel.read(header);
//                }
//                if (!header.hasRemaining()) {
//                    int length = MySQLPacket.getInt(header, 3);
//                    ByteBuffer body = bufferPool.allocate(length);
//                    body.limit(length);
//                    channel.read(body);
//                    if (!body.hasRemaining()) {
//                        bufferList.add(body);
//                        header.reset();
//                        if (length<0xffffff-1){
//                           return true;
//                        }else {
//                            continue;
//                        }
//                    }else {
//                        break;
//                    }
//                }else {
//                    break;
//                }
//            }
//            return true;
//    }

  @Override
  public void writeToChannel(SocketChannel channel) throws IOException {
    applyChannelWritingIndex();
    System.out.println(DumpUtil.dumpAsHex(buffer,buffer.limit()));
    if (channel.write(buffer) == -1) {
      logger.warn("Read EOF ,socket closed ");
      throw new ClosedChannelException();
    }
    channelWriteStartIndex(buffer.position());
  }


  @Override
  public ProxyBufferImpl currentBuffer() {
    return this;
  }

  @Override
  public int packetReadStartIndex() {
    return readStartIndex;
  }

  @Override
  public int packetReadStartIndex(int index) {
    return readStartIndex = index;
  }

  @Override
  public int packetReadEndIndex() {
    return readEndIndex;
  }

  @Override
  public int packetReadEndIndex(int endPos) {

    return readEndIndex = endPos;
  }

  @Override
  public int packetWriteIndex() {
    return buffer.position();
  }

  @Override
  public int packetWriteIndex(int index) {
    buffer.position(index);
    return index;
  }
//
//    @Override
//    public ProxyBufferImpl appendInWriting(ProxyBufferImpl packet) {
//        int added = packet.channelWriteEndIndex() - packet.channelWriteStartIndex();
//        this.compactOrExpendIfNeedRemainsBytesInWriting(added);
//        packet.buffer.position(packet.channelWriteStartIndex());
//        packet.buffer.limit(packet.channelWriteEndIndex());
//        this.buffer.put(packet.buffer);
//        channelWriteEndIndex(channelWriteEndIndex() + added);
//        packet.applyChannelWritingIndex();
//        return this;
//    }

  @Override
  public void expendToLengthIfNeedInReading(int length) {
    if (readEndIndex < length) {
      int max = Math.max(this.buffer.capacity(), length);
      if (max > this.buffer.capacity()) {
        this.buffer = this.bufferPool.expandBuffer(this.buffer, max);
      }
    }
  }

  public void expendToLength(int length) {
    if (this.buffer.capacity() < length) {
      this.buffer = this.bufferPool.expandBuffer(this.buffer, length);
    }
  }

  public void appendLengthIfInReading(int length) {
    if (this.buffer.capacity() < length + readStartIndex) {
      this.buffer = this.bufferPool.expandBuffer(this.buffer, readEndIndex + length);
    }
  }

  @Override
  public void appendLengthIfInReading(int length, boolean c) {
    if (c && readEndIndex < length + readStartIndex) {
      this.buffer = this.bufferPool.expandBuffer(this.buffer, readEndIndex + length);
    }
  }
//
//    @Override
//    public ProxyBufferImpl appendInReading(ProxyBufferImpl packet) {
//        int added = packet.channelReadEndIndex() - packet.channelReadStartIndex();
//        this.compactOrExpendIfNeedRemainsBytesInWriting(added);
//        packet.buffer.position(packet.channelWriteStartIndex());
//        packet.buffer.limit(packet.channelWriteEndIndex());
//        this.buffer.put(packet.buffer);
//        channelWriteEndIndex(channelWriteEndIndex() + added);
//        packet.applyChannelWritingIndex();
//        return this;
//    }


  @Override
  public BufferPool bufferPool() {
    return bufferPool;
  }

  @Override
  public void reset() {
    readStartIndex = 0;
    readEndIndex = 0;
    if (buffer != null) {
      bufferPool.recycle(buffer);
    }
    logger.debug("clear buffer");
    buffer = null;
  }

  @Override
  public void newBuffer() {
    if (buffer != null) {
      throw new MycatExpection("");
    }
    buffer = bufferPool.allocate();
    readStartIndex = 0;
    readEndIndex = 0;
  }

  @Override
  public void newBuffer(byte[] bytes) {
    if (buffer != null) {
      throw new MycatExpection("");
    }
    buffer = ByteBuffer.wrap(bytes);
    readStartIndex = 0;
    readEndIndex = bytes.length;
  }

  @Override
  public void newBuffer(int len) {
    if (buffer != null) {
      throw new MycatExpection("");
    }
    buffer = bufferPool.allocate(len);
    readStartIndex = 0;
    readEndIndex = 0;
  }

//    @Override
//    public void compactInChannelWritingIfNeed() {
//        if (this.channelWriteEndIndex() > buffer.capacity() * (2.0 / 3)) {
//            this.buffer.position(this.channelWriteStartIndex());
//            this.buffer.limit(this.channelWriteEndIndex());
//            this.buffer.compact();
//            this.channelWriteStartIndex(this.buffer.position());
//            this.channelWriteEndIndex(this.buffer.limit());
//        }
//    }

  @Override
  public void compactInChannelReadingIfNeed() {
    if (this.readEndIndex > buffer.capacity() * (1.0 / 3)) {
      this.buffer.position(this.readStartIndex);
      this.buffer.limit(this.readEndIndex);
      this.buffer.compact();
      this.readStartIndex = 0;
      this.readEndIndex = this.buffer.position();
    }
  }
//
//    @Override
//    public void expend(int len) {
//        int position = buffer.position();
//        buffer.position(0);
//        ByteBuffer allocate = bufferPool().allocate(len);
//        allocate.put(buffer);
//        allocate.position(position);
//        bufferPool().recycle(buffer);
//        this.buffer = allocate;
//    }

//    @Override
//    public void compactOrExpendIfNeedRemainsBytesInWriting(int len) {
//        this.compactInChannelWritingIfNeed();
//        int remains = this.channelWriteEndIndex() - this.channelWriteStartIndex();
//        if (remains < len) {
//            expend(this.channelWriteEndIndex() + len);
//        }
//    }

  @Override
  public ProxyBuffer applyChannelWritingIndex() {
    buffer.position(channelWriteStartIndex());
    buffer.limit(channelWriteEndIndex());
    return this;
  }

  @Override
  public void cutRangeBytesInReading(int start, int end) {
    int bpReadStartIndex = this.readStartIndex;
    int bpReadEndIndex = this.readEndIndex;
    if (bpReadStartIndex <= start && start <= end && end <= bpReadEndIndex) {
      ByteBuffer backData = buffer.duplicate();
      backData.position(end);
      backData.limit(bpReadEndIndex);

      this.buffer.position(start);
      this.buffer.put(backData);

      this.readEndIndex -= (end - start);
    } else {
      throw new MycatExpection("");
    }
  }


  @Override
  public ProxyBuffer applyChannelReadingIndex() {
    buffer.position(channelReadStartIndex());
    buffer.limit(buffer.capacity());
    return this;
  }

  @Override
  public void applyChannelWritingIndexForChannelReadingIndex() {
    channelReadStartIndex(channelWriteStartIndex());
    channelReadEndIndex(channelWriteEndIndex());
  }

//    @Override
//    public void compactOrExpendIfNeedRemainsBytesInReading(int len) {
//        this.compactInChannelReadingIfNeed();
//        int remains = this.channelReadEndIndex() - this.channelReadStartIndex();
//        if (remains < len) {
//            expend(this.channelReadEndIndex() + len);
//        }
//    }

  @Override
  public ProxyBuffer newBufferIfNeed() {
    if (buffer == null) {
      newBuffer();
    }
    return this;
  }
}
