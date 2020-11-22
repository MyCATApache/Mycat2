/**
 * Copyright (C) <2020>  <chen junwen>
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

package io.mycat.buffer;

import io.mycat.util.Dumper;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HeapBufferPool implements BufferPool {

    private int chunkSize;

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public ByteBuffer allocate() {
        return ByteBuffer.allocate(chunkSize);
    }

    @Override
    public ByteBuffer allocate(int size) {
        return ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer allocate(byte[] bytes) {
        return ByteBuffer.wrap(Arrays.copyOf(bytes, bytes.length));
    }

    @Override
    public int trace() {
        return 0;
    }

    @Override
    public void recycle(ByteBuffer theBuf) {

    }

    @Override
    public long capacity() {
        return chunkSize;
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public Dumper snapshot() {
        return Dumper.create().addText("chunkSize",chunkSize).addText("trace",0);
    }
}