package io.mycat.mysql;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.buffer.BufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * cjw
 * 294712221@qq.com
 */
public class PacketListToPayloadReader {
    int index = 0;
    final List<ProxyBuffer> multiPackets = new ArrayList<>();

    ByteBuffer curBytebuffer;
    BufferPool bufferPool;
    boolean autoRecycleByteBuffer = false;
    int length;

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }


    public PacketListToPayloadReader() {

    }

    public List<ProxyBuffer> getMultiPackets() {
        return multiPackets;
    }

    public void addBuffer(ProxyBuffer buffer) {
        multiPackets.add(buffer);
        length += buffer.writeIndex - buffer.readIndex - 4;
    }

    public void recyclebuffer(int index) {
        this.multiPackets.remove(index);

    }


    public void clear() {
        length = 0;
        index = 0;
        bufferPool = null;
        curBytebuffer = null;
        autoRecycleByteBuffer = false;
        multiPackets.clear();
    }

    public boolean isClear() {
        if (index == 0 && bufferPool == null && curBytebuffer != null && autoRecycleByteBuffer) {
            return true;
        } else {
            return false;
        }
    }


    private void init(BufferPool bufferPool, boolean autoRecycleByteBuffer) {
        this.bufferPool = bufferPool;
        this.autoRecycleByteBuffer = autoRecycleByteBuffer;
        clear();
    }

    public void loadFirstPacket() {
        loadPacket(multiPackets.get(0));
    }

    private void loadPacket(ProxyBuffer proxyBuffer) {
        curBytebuffer = proxyBuffer.getBuffer();
        curBytebuffer.position(proxyBuffer.readIndex + 4);
        curBytebuffer.limit(proxyBuffer.writeIndex);
        this.curBytebuffer = proxyBuffer.getBuffer();
    }


    public byte get() {
        if (curBytebuffer.hasRemaining()) {
            return curBytebuffer.get();
        } else {
            if (autoRecycleByteBuffer) {
                if (bufferPool != null && curBytebuffer != null) {
                    bufferPool.recycle(curBytebuffer);
                    multiPackets.set(0, null);
                }
            }
            ++this.index;
            if (this.index < multiPackets.size()) {
                loadPacket(multiPackets.get(this.index));
                return get();
            } else {
                throw new RuntimeException("out of size");
            }
        }
    }

    public int length() {
        return length;
    }
}
