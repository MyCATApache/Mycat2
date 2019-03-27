package io.mycat.mysql;

import io.mycat.mysql.packet.PacketSplitter;
import io.mycat.proxy.ProxyBuffer;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * chen jun wen
 * 294712221@qq.com
 */
public class PayloadOnMultiPacket {
    PacketSplitter packetSplitter = new PacketSplitter();
    ByteBuffer byteBuffer;
    private byte packetId;

    public PayloadOnMultiPacket(ByteBuffer byteBuffer, byte packetId) {
        this.byteBuffer = byteBuffer;
        this.packetId = packetId;
        packetSplitter.init(byteBuffer.limit());
    }

    public boolean hasNext() {
        return packetSplitter.nextPacket();
    }

    public void next(ProxyBuffer proxyBuffer) {
        proxyBuffer.writeFixInt(3, packetSplitter.getPacketLen());
        proxyBuffer.writeFixInt(1, packetId++);
        byteBuffer.position(packetSplitter.getOffset());
        byteBuffer.limit(packetSplitter.getOffset() + packetSplitter.getPacketLen());
        proxyBuffer.getBuffer().put(byteBuffer);
        proxyBuffer.writeIndex += packetSplitter.getPacketLen();
    }


    public PacketSplitter getPacketSplitter() {
        return packetSplitter;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public byte getPacketId() {
        return packetId;
    }

}
