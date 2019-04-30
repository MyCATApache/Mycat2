package io.mycat.proxy.packet;

import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.session.Session;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface MySQLPayloadWriter<T extends Session> extends PacketSplitter {


    public abstract int onPayloadLength();

    default public void init(byte packetId, BufferPool bufferPool, SocketChannel serverSocket) {
        setPacketId(packetId);
        setBufferPool(bufferPool);
        setServerSocket(serverSocket);
        setReminsPacketLen(0);
    }

    public abstract MySQLPacket onBuffer(MySQLPacket mySQLPacket, int offset, int packetLength, int reminsPacketLen);

    default public void onStart() {
        try {
            init(onPayloadLength());
            ByteBuffer[] buffer = getBuffer();
            buffer[0] = getBufferPool().allocate(4);
            buffer[1] = null;
            boolean hasNext = false;
            while (hasNext = nextPacketInPacketSplitter()) {
                int offset = getOffsetInPacketSplitter();
                int packetLen = getPacketLenInPacketSplitter();
                setReminsPacketLen(packetLen + 4);
                setPacketId((byte) (1 + getPacketId()));
                writeHeader(buffer[0], getPacketId(), packetLen);
                setCurrentMySQLPacket(onBuffer(getCurrentMySQLPacket(), offset, packetLen, getReminsPacketLen()));
                buffer[1] = getCurrentMySQLPacket().currentBuffer().currentByteBuffer();
                int writed;
                writed = (int) getServerSocket().write(buffer);
                setReminsPacketLen(getReminsPacketLen() - writed);
                int reminsPacketLen = getReminsPacketLen();
                if (reminsPacketLen > 0) {
                    break;
                } else if (reminsPacketLen == 0) {
                    continue;
                }
            }
            if (!hasNext) {
                writeFinishedAndClear(getCurrentMySQLPacket());
            }
        } catch (Exception e) {
            onError(e);
        }
    }

    default public void packetContinueWrite() {
        try {
            int writed = (int) getServerSocket().write(getBuffer());
            setReminsPacketLen(getReminsPacketLen() - writed);
            setOffsetInPacketSplitter(getOffsetInPacketSplitter() + writed);
            while (getReminsPacketLen() == 0) {
                if (nextPacketInPacketSplitter()) {
                    int offset = getOffsetInPacketSplitter();
                    int packetLen = getPacketLenInPacketSplitter();
                    setReminsPacketLen(packetLen + 4);
                    setPacketId((byte) (1 + getPacketId()));
                    ByteBuffer[] buffer = getBuffer();
                    writeHeader(buffer[0], packetLen, getPacketId());
                    setCurrentMySQLPacket(onBuffer(getCurrentMySQLPacket(), offset, packetLen, getReminsPacketLen()));
                    buffer[1] = getCurrentMySQLPacket().currentBuffer().currentByteBuffer();
                     writed = (int) getServerSocket().write(buffer);
                    setReminsPacketLen(getReminsPacketLen() - writed);
                } else {
                    writeFinishedAndClear(getCurrentMySQLPacket());
                }
            }
        } catch (Exception e) {
            onError(e);
        }
    }

    default void writeHeader(ByteBuffer buffer, int packetLen, int packerId) {
        buffer.position(0);
        buffer.put(MySQLPacket.getFixIntByteArray(3, packetLen));
        buffer.put((byte) packerId);
    }

    default void writeFinishedAndClear(MySQLPacket buffer) {
        ByteBuffer[] buffers = getBuffer();
        getBufferPool().recycle(buffers[0]);
        setCurrentMySQLPacket(null);
        setServerSocket(null);
        onWriteFinished(buffer);
    }

    abstract void onWriteFinished(MySQLPacket buffer);

    abstract void onError(Throwable e);

    public byte getPacketId();

    public void setPacketId(byte packetId);

    public BufferPool getBufferPool();

    public void setBufferPool(BufferPool bufferPool);

    public SocketChannel getServerSocket();

    public void setServerSocket(SocketChannel serverSocket);

    public ByteBuffer[] getBuffer();

    public int getReminsPacketLen();

    public void setReminsPacketLen(int reminsPacketLen);

    public MySQLPacket getCurrentMySQLPacket();

    public void setCurrentMySQLPacket(MySQLPacket currentMySQLPacket);

}
