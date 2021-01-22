package io.mycat.util.packet;

public abstract class CommitWritePacket extends AbstractWritePacket {
    @Override
    public Class<? extends AbstractWritePacket> javaClass() {
        return CommitWritePacket.class;
    }
}
