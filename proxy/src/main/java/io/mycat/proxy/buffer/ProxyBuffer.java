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

/**
 * @author chen junwen
 * @date 2019-05-09 02:30
 **/
public interface ProxyBuffer {

  Logger logger = LoggerFactory.getLogger(ProxyBuffer.class);

  /**
   * 获取ByteBuffer
   */
  ByteBuffer currentByteBuffer();

  /**
   * 获取ByteBuffer的容量
   * @return
   */
  int capacity();

  /**
   * 获取Bytebuffer的位置,即channelWritStarteIndex;
   * @return
   */
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

  /**
   * 写入通道时候,数据获取的开始位置
   * @return
   */
  int channelWriteStartIndex();

  /**
   * 写入通道时候,数据获取结束的位置
   * @return
   */
  int channelWriteEndIndex();

  /**
   * 读事件,报文读取类,从该位置开始读取
   * @return
   */
  int channelReadStartIndex();
  /**
   * 读事件,报文读取类,从该位置读取结束
   * @return
   */
  int channelReadEndIndex();

  void channelWriteStartIndex(int index);

  void channelWriteEndIndex(int index);

  void channelReadStartIndex(int index);

  void channelReadEndIndex(int index);

  /**
   * 不改变Proxybuffer的任何下标和数据,扩容bytebuffer
   * @param length
   */
  void expendToLength(int length);

  /**
   * 读事件,从通道读取数据,通道从channelReadStartIndex开始填充数据,直到容量用完
   * @param channel
   * @return
   * @throws IOException
   */
  boolean readFromChannel(SocketChannel channel) throws IOException;

  /**
   * 把Proxybuffer的数据写入通道,从channelWriteStartIndex开始写入
   * @param channel
   * @throws IOException
   */
  void writeToChannel(SocketChannel channel) throws IOException;

  /**
   * 该buffer所在的byteBuffer
   * @return
   */
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
