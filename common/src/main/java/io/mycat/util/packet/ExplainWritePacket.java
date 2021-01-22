package io.mycat.util.packet;

public abstract class ExplainWritePacket extends AbstractWritePacket {
    @Override
    public Class<? extends AbstractWritePacket> javaClass() {
        return ExplainWritePacket.class;
    }
}
