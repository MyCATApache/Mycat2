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

package io.mycat.proxy.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class BufferPoolImpl implements BufferPool {
    @Override
    public ByteBuffer allocate() {
        return ByteBuffer.allocate(128);
    }

    @Override
    public ByteBuffer allocate(int size) {
        return  ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer expandBuffer(ByteBuffer buffer) {
        return expandBuffer(buffer,buffer.capacity()<<1);
    }

    @Override
    public ByteBuffer expandBuffer(ByteBuffer buffer, int len) {
        int position = buffer.position();
        int limit = buffer.limit();

        ByteBuffer allocate = ByteBuffer.allocate(len);
        buffer.position(0);
        buffer.limit(buffer.capacity());
        allocate.put(buffer);
        allocate.position(position);
        allocate.limit(limit);
        return allocate;


    }

    @Override
    public void recycle(ByteBuffer theBuf) {
        theBuf.clear();
    }

    @Override
    public long capacity() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public int getSharedOptsCount() {
        return 0;
    }

    @Override
    public int getChunkSize() {
        return 0;
    }

    @Override
    public ConcurrentHashMap<Long, Long> getNetDirectMemoryUsage() {
        return null;
    }
}
