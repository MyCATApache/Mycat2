/**
 * Copyright (C) <2020>  <jamie12221>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.buffer;

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.Session;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author jamie12221
 * date 2019-05-16 10:09
 **/
public final class ProxyBufferPoolMonitor implements BufferPool {
    final BufferPool bufferPool;

    public ProxyBufferPoolMonitor(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    @Override
    public void init(Map<String, String> args) {

    }

    @Override
    public ByteBuffer allocate() {
        return bufferPool.allocate();
    }

    @Override
    public ByteBuffer allocate(int size) {
        ByteBuffer allocate = bufferPool.allocate(size);
        MycatMonitor.onAllocateByteBuffer(allocate, getSession());
        return allocate;
    }

    public Session getSession() {
        Thread thread1 = Thread.currentThread();
        if (thread1 instanceof SessionThread){
            return ((SessionThread) thread1).getCurSession();
        }
        return null;
    }

    @Override
    public ByteBuffer allocate(byte[] bytes) {
        return bufferPool.allocate(bytes);
    }

    @Override
    public ByteBuffer expandBuffer(ByteBuffer old, int len) {
        int chunkSize = bufferPool.chunkSize();
        ByteBuffer byteBuffer = bufferPool.expandBuffer(old, (len / chunkSize + 1) * chunkSize);
        MycatMonitor.onExpandByteBuffer(byteBuffer, getSession());
        return byteBuffer;
    }

    @Override
    public void recycle(ByteBuffer theBuf) {
        MycatMonitor.onRecycleByteBuffer(theBuf, getSession());
        bufferPool.recycle(theBuf);
    }

    @Override
    public long capacity() {
        return bufferPool.capacity();
    }

    @Override
    public int chunkSize() {
        return bufferPool.chunkSize();
    }
}
