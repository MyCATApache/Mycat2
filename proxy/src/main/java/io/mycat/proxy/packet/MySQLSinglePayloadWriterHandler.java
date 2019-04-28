package io.mycat.proxy.packet;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.session.AbstractSession;
import io.mycat.proxy.session.Session;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class MySQLSinglePayloadWriterHandler<T extends AbstractSession<T>> implements MySQLPayloadWriter<T> , NIOHandler<T> {
    MySQLPacket mySQLPacket;
    int packetId = 0;
    BufferPool bufferPool;
    SocketChannel socketChannel;
    ByteBuffer[] buffers =  new ByteBuffer[2];
    int reminsPacketLen;
    MySQLPacket currentMySQLPacket;
    int totalSize;
    int currentPacketLen;
    int offset;
    @Override
    public int onPayloadLength() {
        return mySQLPacket.packetWriteIndex();
    }

    @Override
    public MySQLPacket onBuffer(MySQLPacket mySQLPacket, int offset, int packetLength, int reminsPacketLen) {
        return mySQLPacket;
    }

    @Override
    public void onWriteFinished(MySQLPacket buffer) {

    }

    @Override
    public void onError(Throwable e) {

    }

    @Override
    public byte getPacketId() {
        return (byte) packetId;
    }

    @Override
    public void setPacketId(byte packetId) {
        this.packetId = packetId;
    }

    @Override
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    @Override
    public void setBufferPool(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    @Override
    public SocketChannel getServerSocket() {
        return socketChannel;
    }

    @Override
    public void setServerSocket(SocketChannel serverSocket) {
        this.socketChannel = serverSocket;
    }

    @Override
    public ByteBuffer[] getBuffer() {
        return buffers;
    }

    @Override
    public int getReminsPacketLen() {
        return reminsPacketLen;
    }

    @Override
    public void setReminsPacketLen(int reminsPacketLen) {
        this.reminsPacketLen = reminsPacketLen;
    }

    @Override
    public MySQLPacket getCurrentMySQLPacket() {
        return currentMySQLPacket;
    }

    @Override
    public void setCurrentMySQLPacket(MySQLPacket currentMySQLPacket) {
        this.currentMySQLPacket = currentMySQLPacket;
    }

    @Override
    public int getTotalSizeInPacketSplitter() {
        return totalSize;
    }

    @Override
    public void setTotalSizeInPacketSplitter(int totalSize) {
        this.totalSize = totalSize;
    }

    @Override
    public int getCurrentPacketLenInPacketSplitter() {
        return currentPacketLen;
    }

    @Override
    public void setCurrentPacketLenInPacketSplitter(int currentPacketLen) {
        this.currentPacketLen = currentPacketLen;
    }

    @Override
    public void setOffsetInPacketSplitter(int offset) {
        this.offset = offset;
    }


    @Override
    public int getOffsetInPacketSplitter() {
        return offset;
    }
}
