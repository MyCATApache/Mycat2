package io.mycat.util.packet;

public abstract class BeginWritePacket extends AbstractWritePacket {

    @Override
    public Class<? extends AbstractWritePacket> javaClass() {
        return BeginWritePacket.class;
    }
}
