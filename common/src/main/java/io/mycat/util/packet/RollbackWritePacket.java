package io.mycat.util.packet;

public abstract class RollbackWritePacket extends AbstractWritePacket {
    @Override
    public Class<? extends AbstractWritePacket> javaClass() {
        return RollbackWritePacket.class;
    }
}
