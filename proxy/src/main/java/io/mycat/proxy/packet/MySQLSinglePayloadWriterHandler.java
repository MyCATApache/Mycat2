/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
    public int getPacketLenInPacketSplitter() {
        return currentPacketLen;
    }

    @Override
    public void setPacketLenInPacketSplitter(int currentPacketLen) {
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
