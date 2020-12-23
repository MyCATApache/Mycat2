package io.mycat.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DirectByteBufferPool
 *
 * @author wuzhih
 * @author zagnix
 */
@SuppressWarnings("restriction")
public class MycatDirectByteBufferPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatDirectByteBufferPool.class);
    private final int chunkSize;
    private final int pageSize;
    private final short pageCount;
    private ByteBufferPage[] allPages;
    private AtomicLong prevAllocatedPage;

    public MycatDirectByteBufferPool(int pageSize, short chunkSize, short pageCount) {
        allPages = new ByteBufferPage[pageCount];
        this.chunkSize = chunkSize;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        prevAllocatedPage = new AtomicLong(0);
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
    }

    public ByteBuffer allocate() {
        return allocate(chunkSize);
    }

    public ByteBuffer allocate(int size) {
        final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        int selectedPage = (int) (prevAllocatedPage.incrementAndGet() % allPages.length);
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        }

        if (byteBuf == null) {
            LOGGER.warn("can't allocate DirectByteBuffer from DirectByteBufferPool. Please pay attention to whether it is a memory leak or there is no enough direct memory.");
            return ByteBuffer.allocate(size);
        }
        return byteBuf;
    }


    public ByteBuffer allocate(byte[] bytes) {
        ByteBuffer allocate = allocate(bytes.length);
        allocate.put(bytes);
        allocate.position(0);
        allocate.limit(bytes.length);
        return allocate;
    }

    public void recycle(ByteBuffer theBuf) {
        if (!(theBuf.isDirect())) {
            theBuf.clear();
            return;
        }

        boolean recycled = false;
        DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
        int chunkCount = theBuf.capacity() / chunkSize;
        DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
        int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / this.chunkSize);
        for (ByteBufferPage allPage : allPages) {
            if ((recycled = allPage.recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount))) {
                break;
            }
        }
        if (!recycled) {
            LOGGER.info("warning ,not recycled buffer " + theBuf);
        }
    }

    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            ByteBuffer buffer = allPages[i].allocateChunk(theChunkCount);
            if (buffer != null) {
                prevAllocatedPage.getAndSet(i);
                return buffer;
            }
        }
        return null;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * return the total size of the buffer memory
     *
     * @return long
     */
    public long capacity() {
        return (long) pageSize * pageCount;
    }

    /**
     * return the remain free part of memory
     *
     * @return long
     */
    public long size() {
        long usage = usage();
        return this.capacity() - usage;
    }

    public long usage() {
        long usage = 0L;
        for (ByteBufferPage page : allPages) {
            usage += page.getUsage();
        }
        return usage;
    }
}
