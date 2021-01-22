package io.mycat.util.packet;

public abstract class BeginWritePacket extends AbstractSocketWritePacket {

    @Override
    public Class<? extends AbstractSocketWritePacket> javaClass() {
        return BeginWritePacket.class;
    }
}
