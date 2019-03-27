package io.mycat.mysql;

import io.mycat.mysql.packet.PacketSplitter;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.buffer.BufferPool;

import java.nio.ByteBuffer;
import java.util.List;

public class PayloadListToPacketWriter {
    PacketSplitter packetSplitter = new PacketSplitter();
    BufferPool bufferPool;
    int size;
    private List<ByteBuffer> byteBuffers;
    int packetId;
    boolean hasNext = true;
    int index = 0;
    boolean autoRecycleByteBuffer = true;

    public PayloadListToPacketWriter() {
    }

    public void init(BufferPool bufferPool,List<ByteBuffer> byteBuffers,boolean autoRecycleByteBuffer, int packetId, int size) {
        this.bufferPool = bufferPool;
        this.byteBuffers = byteBuffers;
        this.autoRecycleByteBuffer = autoRecycleByteBuffer;
        this.packetId = packetId;
        this.size = size;
        packetSplitter.init(size);
        index = 0;
    }


    public boolean hasNext() {
        return hasNext;
    }


    public void next(ProxyBuffer buffer) {
        buffer.reset();
        hasNext = packetSplitter.nextPacket();
        int payloadLen = packetSplitter.getPacketLen();
        int packetLength = payloadLen + 4;
        if (buffer.getBuffer().capacity() < packetLength) {
            MySQLProxyPacketResolver.simpleAdjustCapacityProxybuffer(buffer,packetLength);
        }
        ByteBuffer byteBuffer = byteBuffers.get(index);
        buffer.writeFixInt(3,payloadLen);
        buffer.writeByte((byte)(packetId++));
        for (int i = 0; i < payloadLen; i++) {
            if (byteBuffer.hasRemaining()){
                buffer.writeByte(byteBuffer.get());
            }else {
                if (autoRecycleByteBuffer){
                    bufferPool.recycle(byteBuffer);
                }
                byteBuffer = byteBuffers.get(index++);
            }
        }
    }
}
