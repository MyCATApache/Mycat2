package io.mycat.proxy.packet;

/**
 * copy form com.mysql.cj.protocol.a
 */
public interface PacketSplitter {
    // static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;
    public static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;

    default void init(int totalSize) {
        setTotalSizeInPacketSplitter(totalSize);
        setPacketLenInPacketSplitter(0);
        setOffsetInPacketSplitter(0);
    }

    default boolean nextPacketInPacketSplitter() {
        setOffsetInPacketSplitter(getOffsetInPacketSplitter() + getPacketLenInPacketSplitter());
        // need a zero-len packet if final packet len is MAX_PACKET_SIZE
        if (getPacketLenInPacketSplitter() == MAX_PACKET_SIZE && getOffsetInPacketSplitter() == getTotalSizeInPacketSplitter()) {
            setPacketLenInPacketSplitter(0);
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
            setPacketLenInPacketSplitter(MAX_PACKET_SIZE);
        } else {
            setPacketLenInPacketSplitter(currentPacketLen);
        }
        return offset < totalSize;
    }

    public int getTotalSizeInPacketSplitter();

    public void setTotalSizeInPacketSplitter(int totalSize);

    public int getPacketLenInPacketSplitter();

    public void setPacketLenInPacketSplitter(int currentPacketLen);

    public void setOffsetInPacketSplitter(int offset);

    public int getOffsetInPacketSplitter();

}
