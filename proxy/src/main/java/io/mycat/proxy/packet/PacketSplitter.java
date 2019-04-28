package io.mycat.proxy.packet;

/**
 * copy form com.mysql.cj.protocol.a
 */
public interface PacketSplitter {
    // static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;
    public static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;

    default public void init(int totalSize) {
        setTotalSizeInPacketSplitter(totalSize);
        setCurrentPacketLenInPacketSplitter(0);
        setOffsetInPacketSplitter(0);
    }

    default public boolean nextPacketInPacketSplitter() {
        setOffsetInPacketSplitter(getOffsetInPacketSplitter() + getCurrentPacketLenInPacketSplitter());
        // need a zero-len packet if final packet len is MAX_PACKET_SIZE
        if (getCurrentPacketLenInPacketSplitter() == MAX_PACKET_SIZE && getOffsetInPacketSplitter() == getTotalSizeInPacketSplitter()) {
            setCurrentPacketLenInPacketSplitter(0);
            return true;
        }

        // allow empty packets
        if (getTotalSizeInPacketSplitter() == 0) {
            setTotalSizeInPacketSplitter(-1); // to return `false' next iteration
            return true;
        }

        int currentPacketLen;
        int offset = getOffsetInPacketSplitter();
        int totalSize = getTotalSizeInPacketSplitter();
        currentPacketLen = getTotalSizeInPacketSplitter() - offset;
        if (currentPacketLen > MAX_PACKET_SIZE) {
            setCurrentPacketLenInPacketSplitter(MAX_PACKET_SIZE);
        } else {
            setCurrentPacketLenInPacketSplitter(currentPacketLen);
        }
        return offset < totalSize;
    }

    public int getTotalSizeInPacketSplitter();

    public void setTotalSizeInPacketSplitter(int totalSize);

    public int getCurrentPacketLenInPacketSplitter();

    public void setCurrentPacketLenInPacketSplitter(int currentPacketLen);

    public void setOffsetInPacketSplitter(int offset);

    public int getOffsetInPacketSplitter();

}
