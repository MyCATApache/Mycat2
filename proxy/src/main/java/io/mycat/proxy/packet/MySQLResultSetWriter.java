package io.mycat.proxy.packet;

import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.session.MycatSession;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class MySQLResultSetWriter implements MySQLPayloadWriter<MycatSession> {

    @Override
    public int onPayloadLength() {
        return 0;
    }

    @Override
    public MySQLPacket onBuffer(MySQLPacket mySQLPacket, int offset, int packetLength, int reminsPacketLen) {
        return null;
    }

    @Override
    public void onWriteFinished(MySQLPacket buffer) {

    }

    @Override
    public void onError(Throwable e) {

    }

    @Override
    public byte getPacketId() {
        return 0;
    }

    @Override
    public void setPacketId(byte packetId) {

    }

    @Override
    public BufferPool getBufferPool() {
        return null;
    }

    @Override
    public void setBufferPool(BufferPool bufferPool) {

    }

    @Override
    public SocketChannel getServerSocket() {
        return null;
    }

    @Override
    public void setServerSocket(SocketChannel serverSocket) {

    }

    @Override
    public ByteBuffer[] getBuffer() {
        return new ByteBuffer[0];
    }

    @Override
    public int getReminsPacketLen() {
        return 0;
    }

    @Override
    public void setReminsPacketLen(int reminsPacketLen) {

    }

    @Override
    public MySQLPacket getCurrentMySQLPacket() {
        return null;
    }

    @Override
    public void setCurrentMySQLPacket(MySQLPacket currentMySQLPacket) {

    }

    @Override
    public int getTotalSizeInPacketSplitter() {
        return 0;
    }

    @Override
    public void setTotalSizeInPacketSplitter(int totalSize) {

    }

    @Override
    public int getCurrentPacketLenInPacketSplitter() {
        return 0;
    }

    @Override
    public void setCurrentPacketLenInPacketSplitter(int currentPacketLen) {

    }

    @Override
    public void setOffsetInPacketSplitter(int offset) {

    }


    @Override
    public int getOffsetInPacketSplitter() {
        return 0;
    }
}
