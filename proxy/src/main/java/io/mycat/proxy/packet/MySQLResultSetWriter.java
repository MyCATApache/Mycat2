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
    public int getPacketLenInPacketSplitter() {
        return 0;
    }

    @Override
    public void setPacketLenInPacketSplitter(int currentPacketLen) {

    }

    @Override
    public void setOffsetInPacketSplitter(int offset) {

    }


    @Override
    public int getOffsetInPacketSplitter() {
        return 0;
    }
}
