/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.proxy.buffer;

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.packet.MySQLPacket;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ProxyBuffer {

  Logger logger = LoggerFactory.getLogger(ProxyBuffer.class);

  ByteBuffer currentByteBuffer();

  int capacity();

  int position();

  int position(int index);

  void writeFloat(float f);

  float readFloat();

  MySQLPacket writeLong(long l);

  long readLong();

  MySQLPacket writeDouble(double d);

  double readDouble();

  byte get();

  byte get(int index);

  ProxyBuffer get(byte[] bytes);

  byte put(byte b);

  void put(byte[] bytes);

  void put(byte[] bytes, int offset, int legnth);

  int channelWriteStartIndex();

  int channelWriteEndIndex();

  int channelReadStartIndex();

  int channelReadEndIndex();

  void channelWriteStartIndex(int index);

  void channelWriteEndIndex(int index);

  void channelReadStartIndex(int index);

  void channelReadEndIndex(int index);

  void expendToLength(int length);

  boolean readFromChannel(SocketChannel channel) throws IOException;

  void writeToChannel(SocketChannel channel) throws IOException;

  BufferPool bufferPool();

  void reset();

  void newBuffer();

  void newBuffer(byte[] bytes);

  void newBuffer(int len);

  void expendToLengthIfNeedInReading(int length);

  void appendLengthIfInReading(int length);

  void appendLengthIfInReading(int length, boolean c);

  void compactInChannelReadingIfNeed();


  ProxyBuffer newBufferIfNeed();


  default boolean channelWriteFinished() {
    return channelWriteStartIndex() == channelWriteEndIndex();
  }

  default boolean channelReadFinished() {
    return channelReadStartIndex() == channelReadEndIndex();
  }

  ProxyBuffer applyChannelWritingIndex();

  void cutRangeBytesInReading(int start, int end);

  ProxyBuffer applyChannelReadingIndex();

  void applyChannelWritingIndexForChannelReadingIndex();

  int remainsInReading();
}
