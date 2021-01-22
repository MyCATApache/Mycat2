package io.mycat.util.packet;

public abstract class ExplainWritePacket extends AbstractSocketWritePacket {
    @Override
    public Class<? extends AbstractSocketWritePacket> javaClass() {
        return ExplainWritePacket.class;
    }
}
