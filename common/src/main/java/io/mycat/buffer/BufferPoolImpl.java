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

package io.mycat.buffer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import sun.nio.ch.DirectBuffer;

public class BufferPoolImpl implements BufferPool {

  private final int chunkSize;
  private final int pageSize;
  private final int pageCount;
  /**
   * 记录对线程ID->该线程的所使用Direct Buffer的size
   */
  private final HashMap<Long, Long> memoryUsage;
  private ByteBufferPage[] allPages;
  // private int prevAllocatedPage = 0;
  private AtomicInteger prevAllocatedPage;

  public BufferPoolImpl(int pageSize, int chunkSize, int pageCount) {
    allPages = new ByteBufferPage[pageCount];
    this.chunkSize = chunkSize;
    this.pageSize = pageSize;
    this.pageCount = pageCount;
    prevAllocatedPage = new AtomicInteger(0);
    for (int i = 0; i < pageCount; i++) {
      allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
    }
    memoryUsage = new HashMap<>();
  }


  @Override
  public ByteBuffer allocate() {
    return allocate(getChunkSize());
  }

  @Override
  public ByteBuffer allocate(int size) {
    final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
    // 如果大于一个chunk 的大小.分配堆内 内存,用完释放, 不再 使用堆外内存
    if (theChunkCount > 1) {
      return ByteBuffer.allocate(size);
    }
    int selectedPage = prevAllocatedPage.incrementAndGet() % allPages.length;
    ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
    if (byteBuf == null) {
      byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
    }
    final long threadId = Thread.currentThread().getId();

    if (byteBuf != null) {
      if (memoryUsage.containsKey(threadId)) {
        memoryUsage.put(threadId, memoryUsage.get(threadId) + byteBuf.capacity());
      } else {
        memoryUsage.put(threadId, (long) byteBuf.capacity());
      }
    }

    //如果堆外内存，没有可用空间,分配 堆内内存,一段时间后,还在使用,看情况再转成堆外内存
    if (byteBuf == null) {
      return ByteBuffer.allocate(size);
    }
    byteBuf.position(0);
    byteBuf.limit(byteBuf.capacity());
    return byteBuf;
  }

  private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
    for (int i = startPage; i < endPage; i++) {
      ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
      if (buffer != null) {
        prevAllocatedPage.getAndSet(i);
        return buffer;
      }
    }
    return null;
  }

  @Override
  public ByteBuffer allocate(byte[] bytes) {
    ByteBuffer allocate = allocate(bytes.length);
    allocate.put(bytes);
    allocate.flip();
    return allocate;
  }

  @Override
  public ByteBuffer expandBuffer(ByteBuffer buffer) {
    return expandBuffer(buffer, buffer.capacity() << 1);
  }

  @Override
  public ByteBuffer expandBuffer(ByteBuffer old, int len) {
    assert old != null;
    assert len != 0 && len > old.capacity();

    int position = old.position();
    int limit = old.limit();

    ByteBuffer newBuffer = allocate(len);
    old.position(0);
    old.limit(old.capacity());
    newBuffer.put(old);
    newBuffer.position(position);
    newBuffer.limit(limit);

    recycle(old);

    return newBuffer;


  }

  @Override
  public void recycle(ByteBuffer theBuf) {
    assert theBuf != null;

    if (!(theBuf instanceof DirectBuffer)) {
      return;
    }

    final long size = theBuf.capacity();

    boolean recycled = false;
    DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
    int chunkCount = theBuf.capacity() / chunkSize;
    DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
    int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / this.chunkSize);
    for (int i = 0; i < allPages.length; i++) {
      if ((recycled =
               allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount) == true)) {
        break;
      }
    }
    final long threadId = Thread.currentThread().getId();

    if (memoryUsage.containsKey(threadId)) {
      memoryUsage.put(threadId, memoryUsage.get(threadId) - size);
    }
    assert (recycled == true);
  }

  @Override
  public long capacity() {
    return (long) pageSize * pageCount;
  }

  @Override
  public long size() {
    return (long) pageSize * chunkSize * pageCount;
  }


  @Override
  public int getChunkSize() {
    return chunkSize;
  }

  @Override
  public Map<Long, Long> getNetDirectMemoryUsage() {
    return this.memoryUsage;
  }

  /*
   * 用来保存一个一个ByteBuffer为底层存储的内存页
   */
  @SuppressWarnings("restriction")
  public class ByteBufferPage {

    private final ByteBuffer buf;
    private final int chunkSize;
    private final int chunkCount;
    private final BitSet chunkAllocateTrack;
    private final AtomicBoolean allocLockStatus = new AtomicBoolean(false);
    private final long startAddress;

    public ByteBufferPage(ByteBuffer buf, int chunkSize) {
      super();
      this.chunkSize = chunkSize;
      chunkCount = buf.capacity() / chunkSize;
      chunkAllocateTrack = new BitSet(chunkCount);
      this.buf = buf;
      startAddress = ((sun.nio.ch.DirectBuffer) buf).address();
    }

    public ByteBuffer allocatChunk(int theChunkCount) {
      if (!allocLockStatus.compareAndSet(false, true)) {
        return null;
      }
      int startChunk = -1;
      int contiueCount = 0;
      try {
        for (int i = 0; i < chunkCount; i++) {
          if (chunkAllocateTrack.get(i) == false) {
            if (startChunk == -1) {
              startChunk = i;
              contiueCount = 1;
              if (theChunkCount == 1) {
                break;
              }
            } else {
              if (++contiueCount == theChunkCount) {
                break;
              }
            }
          } else {
            startChunk = -1;
            contiueCount = 0;
          }
        }
        if (contiueCount == theChunkCount) {
          int offStart = startChunk * chunkSize;
          int offEnd = offStart + theChunkCount * chunkSize;
          buf.limit(offEnd);
          buf.position(offStart);

          ByteBuffer newBuf = buf.slice();
          //sun.nio.ch.DirectBuffer theBuf = (DirectBuffer) newBuf;
          //System.out.println("offAddress " + (theBuf.address() - startAddress));
          markChunksUsed(startChunk, theChunkCount);
          return newBuf;
        } else {
          //System.out.println("contiueCount " + contiueCount + " theChunkCount " + theChunkCount);
          return null;
        }
      } finally {
        allocLockStatus.set(false);
      }
    }

    private void markChunksUsed(int startChunk, int theChunkCount) {
      for (int i = 0; i < theChunkCount; i++) {
        chunkAllocateTrack.set(startChunk + i);
      }
    }

    private void markChunksUnused(int startChunk, int theChunkCount) {
      for (int i = 0; i < theChunkCount; i++) {
        chunkAllocateTrack.clear(startChunk + i);
      }
    }

    public boolean recycleBuffer(ByteBuffer parent, int startChunk, int chunkCount) {

      if (parent == this.buf) {

        while (!this.allocLockStatus.compareAndSet(false, true)) {
          Thread.yield();
        }
        try {
          markChunksUnused(startChunk, chunkCount);
        } finally {
          allocLockStatus.set(false);
        }
        return true;
      }
      return false;
    }
  }

}
