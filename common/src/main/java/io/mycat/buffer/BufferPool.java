package io.mycat.buffer;

/**
 * Copyright (C) <2019>  <Hash Zhang>
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

import io.mycat.Dumpable;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 缓冲池
 *
 * @author Hash Zhang
 * @version 1.0
 * time 12:19 2016/5/23
 */
public interface BufferPool  extends Dumpable {

    public void init(Map<String,String> args);

    ByteBuffer allocate();

    ByteBuffer allocate(int size);

    ByteBuffer allocate(byte[] bytes);

    int trace();


    public default ByteBuffer expandBuffer(ByteBuffer old, int len) {
        assert old != null;
        assert len != 0 && len > old.capacity();

        int position = old.position();
        int limit = old.limit();

        ByteBuffer newBuffer = allocate(len);
        old.position(0);
        old.limit(old.capacity());
        newBuffer.put(old);
        newBuffer.position(position);
        newBuffer.limit(limit);

        recycle(old);
        return newBuffer;
    }

    void recycle(ByteBuffer theBuf);

    long capacity();

  int chunkSize();
//
//    Map<Long, Long> getNetDirectMemoryUsage();
}
