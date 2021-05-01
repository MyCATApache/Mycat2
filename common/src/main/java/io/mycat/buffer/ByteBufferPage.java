/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.buffer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * ByteBufferPage
 */
@SuppressWarnings("restriction")
public class ByteBufferPage {

    private final ByteBuffer buf;
    private final int chunkSize;
    private final int chunkCount;
    private final BitSet chunkAllocateTrack;
    private final AtomicBoolean allocLockStatus = new AtomicBoolean(false);

    public ByteBufferPage(ByteBuffer buf, int chunkSize) {
        super();
        this.chunkSize = chunkSize;
        chunkCount = buf.capacity() / chunkSize;
        chunkAllocateTrack = new BitSet(chunkCount);
        this.buf = buf;
    }

    public ByteBuffer allocateChunk(int theChunkCount) {
        if (!allocLockStatus.compareAndSet(false, true)) {
            return null;
        }
        int startChunk = -1;
        int continueCount = 0;
        try {
            for (int i = 0; i < chunkCount; i++) {
                if (!chunkAllocateTrack.get(i)) {
                    if (startChunk == -1) {
                        startChunk = i;
                        continueCount = 1;
                        if (theChunkCount == 1) {
                            break;
                        }
                    } else {
                        if (++continueCount == theChunkCount) {
                            break;
                        }
                    }
                } else {
                    startChunk = -1;
                    continueCount = 0;
                }
            }
            if (continueCount == theChunkCount) {
                int offStart = startChunk * chunkSize;
                int offEnd = offStart + theChunkCount * chunkSize;
                buf.limit(offEnd);
                buf.position(offStart);

                ByteBuffer newBuf = buf.slice();
                //sun.nio.ch.DirectBuffer theBuf = (DirectBuffer) newBuf;
                //System.out.println("offAddress " + (theBuf.address() - startAddress));
                markChunksUsed(startChunk, theChunkCount);
                return newBuf;
            } else {
                //System.out.println("contiue Count " + contiueCount + " theChunkCount " + theChunkCount);
                return null;
            }
        } finally {
            allocLockStatus.set(false);
        }
    }

    private void markChunksUsed(int startChunk, int theChunkCount) {
        for (int i = 0; i < theChunkCount; i++) {
            chunkAllocateTrack.set(startChunk + i);
        }
    }

    private void markChunksUnused(int startChunk, int theChunkCount) {
        for (int i = 0; i < theChunkCount; i++) {
            chunkAllocateTrack.clear(startChunk + i);
        }
    }

    public boolean recycleBuffer(ByteBuffer parent, int startChunk, int chunkNum) {

        if (parent == this.buf) {

            while (!this.allocLockStatus.compareAndSet(false, true)) {
                Thread.yield();
            }
            try {
                markChunksUnused(startChunk, chunkNum);
            } finally {
                allocLockStatus.set(false);
            }
            return true;
        }
        return false;
    }

    public long getUsage() {
        return chunkAllocateTrack.cardinality() * (long) chunkSize;
    }
}
