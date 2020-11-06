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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HeapBufferPool implements BufferPool {

    private int chunkSize;
    private int pageSize;
    private int pageCount;

    public final static String CHUNK_SIZE = "chunkSize";
    public final static String PAGE_SIZE = "pageSize";
    public final static String PAGE_COUNT = "pageCount";

    private final AtomicInteger trace = new AtomicInteger(0);

    @Override
    public void init(Map<String, String> args) {
        String chunkSizeText = args.get(CHUNK_SIZE);
        String pageSizeText = args.get(PAGE_SIZE);
        String pageCountText = args.get(PAGE_COUNT);

        if (chunkSizeText == null) {
            this.chunkSize = 8192;
        } else {
            this.chunkSize = Integer.parseInt(chunkSizeText);
        }

//        this.chunkSize = Integer.parseInt(chunkSizeText);
//        this.pageSize = Integer.parseInt(pageSizeText);
//        this.pageCount = Integer.parseInt(pageCountText);
    }

    @Override
    public ByteBuffer allocate() {
        trace.incrementAndGet();
        return ByteBuffer.allocate(chunkSize);
    }

    @Override
    public ByteBuffer allocate(int size) {
        trace.incrementAndGet();
        return ByteBuffer.allocate(size);
    }

    @Override
    public ByteBuffer allocate(byte[] bytes) {
        trace.incrementAndGet();
        return ByteBuffer.wrap(Arrays.copyOf(bytes, bytes.length));
    }

    @Override
    public int trace() {
        return trace.get();
    }

    @Override
    public void recycle(ByteBuffer theBuf) {
        trace.decrementAndGet();
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
        return Dumper.create().addText("chunkSize",chunkSize).addText("trace",trace.get());
    }
}