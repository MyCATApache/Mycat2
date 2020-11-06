/**
 * Copyright (C) <2020>  <jamie12221>
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

import io.mycat.MycatException;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.reactor.SessionThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 封装,记录buffer在跨线程的工具类 junwen12221
 */
public class CrossSwapThreadBufferPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrossSwapThreadBufferPool.class);
  private volatile SessionThread source;
  private BufferPool bufferPool;

  public CrossSwapThreadBufferPool(
      BufferPool bufferPool) {
    this.bufferPool = bufferPool;
  }

  public ByteBuffer allocate(int size) {
    check();
    return bufferPool.allocate(size);
  }

  public ByteBuffer allocate(byte[] bytes) {
    check();
    return bufferPool.allocate(bytes);
  }

  private void check() {
    if (source != null && source != Thread.currentThread()) {
      LOGGER.error("{}", Thread.currentThread());
      throw new MycatException("Illegal state");
    }
  }

  public void recycle(ByteBuffer theBuf) {
    bufferPool.recycle(theBuf);
  }

  public void bindSource(SessionThread source) {
    this.source = source;
  }

  public SessionThread getSource() {
    return source;
  }
}