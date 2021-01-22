package io.mycat.util.packet;

public abstract class CommitWritePacket extends AbstractSocketWritePacket {
    @Override
    public Class<? extends AbstractSocketWritePacket> javaClass() {
        return CommitWritePacket.class;
    }
}
