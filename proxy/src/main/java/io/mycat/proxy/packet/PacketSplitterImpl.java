package io.mycat.proxy.packet;

public class PacketSplitterImpl implements PacketSplitter {
    int totalSize;
    int currentPacketLen;
    int offset;
    int paketLen;
    @Override
    public int getTotalSizeInPacketSplitter() {
        return totalSize;
    }

    @Override
    public void setTotalSizeInPacketSplitter(int totalSize) {
        this.totalSize = totalSize;
    }

    @Override
    public int getPacketLenInPacketSplitter() {
        return currentPacketLen;
    }

    @Override
    public void setPacketLenInPacketSplitter(int currentPacketLen) {
        this.currentPacketLen = currentPacketLen;
    }

    @Override
    public void setOffsetInPacketSplitter(int offset) {
        this.offset  = offset;
    }



    @Override
    public int getOffsetInPacketSplitter() {
        return paketLen;
    }
}
