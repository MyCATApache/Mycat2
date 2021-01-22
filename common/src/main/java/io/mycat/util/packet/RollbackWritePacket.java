package io.mycat.util.packet;

public abstract class RollbackWritePacket extends AbstractSocketWritePacket {
    @Override
    public Class<? extends AbstractSocketWritePacket> javaClass() {
        return RollbackWritePacket.class;
    }
}
