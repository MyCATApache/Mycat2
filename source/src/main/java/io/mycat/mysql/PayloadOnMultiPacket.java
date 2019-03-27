package io.mycat.mysql;

import io.mycat.mysql.packet.PacketSplitter;
import io.mycat.proxy.buffer.BufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PayloadOnMultiPacket {
    BufferPool bufferPool;
    List<ByteBuffer> bufferList = new ArrayList<>();
    private final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;
    ByteBuffer currentBuffer;
    int size = 0;
    int index = 0;
    int packetId = 0;
    int rest;
    PacketSplitter packetSplitter = new PacketSplitter();

    public void add(byte b) {
        if (bufferList.isEmpty()) {
            currentBuffer = bufferList.set(0, bufferPool.allocate(MAX_PACKET_SIZE));
        } else if (!currentBuffer.hasRemaining()) {
            currentBuffer = bufferList.set(bufferList.size(), bufferPool.allocate(MAX_PACKET_SIZE));
        }
        currentBuffer.put(b);
        size++;
    }


    public PayloadOnMultiPacket(BufferPool bufferPool,int size) {
        this.bufferPool = bufferPool;
        this.size = size;
        packetSplitter.init(size);
    }


    public byte get() {
        if (bufferList.isEmpty()) {
            throw new RuntimeException("");
        } else if (currentBuffer == null) {
            index = 0;
            currentBuffer = bufferList.get(0);
            currentBuffer.position(0);
            return currentBuffer.get();
        } else if (currentBuffer.hasRemaining()) {
            return currentBuffer.get();
        } else {
            currentBuffer = bufferList.get(++index);
            currentBuffer.position(0);
            return get();
        }
    }


    public int length() {
        return size;
    }
}
