package io.mycat.lib.impl;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatResultSetType;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author chen junwen
 */
public class ByteBufferResponseRecorder implements MycatResultSetResponse {
    private final ResultSetCacheRecorder cache;
    private final MycatResultSetResponse resultSetResponse;
    private final Runnable runnable;


    public ByteBufferResponseRecorder(ResultSetCacheRecorder cache, MycatResultSetResponse resultSetResponse, Runnable runnable) {
        this.cache = cache;
        this.resultSetResponse = resultSetResponse;
        this.runnable = runnable;
    }

    @Override
    public MycatResultSetType getType() {
        return MycatResultSetType.RRESULTSET;
    }

    @Override
    public int columnCount() {
        int columnCount = resultSetResponse.columnCount();
        cache.startRecordColumn(columnCount);
        return columnCount;
    }

    @Override
    public Iterator<byte[]> columnDefIterator() {
        Iterator<byte[]> iterator = this.resultSetResponse.columnDefIterator();
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                boolean b = iterator.hasNext();
                if (!b) {
                    cache.startRecordRow();
                }
                return b;
            }

            @Override
            public byte[] next() {
                byte[] next = iterator.next();
                cache.addColumnDefBytes(next);
                return next;
            }
        };
    }

    @Override
    public Iterator<byte[]> rowIterator() {
        Iterator<byte[]> iterator = this.resultSetResponse.rowIterator();
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public byte[] next() {
                byte[] next = iterator.next();
                cache.addRowBytes(next);
                return next;
            }
        };
    }

    @Override
    public void close() throws IOException {
        cache.sync();
        runnable.run();
    }
    public void cache() {
        ByteBufferResponseRecorder byteBufferResponseRecorder = this;
        byteBufferResponseRecorder.columnCount();
        Iterator<byte[]> iterator = byteBufferResponseRecorder.columnDefIterator();
        while (iterator.hasNext()) {
            iterator.next();
        }
        Iterator<byte[]> iterator1 = byteBufferResponseRecorder.rowIterator();
        while (iterator1.hasNext()) {
            iterator1.next();
        }
        try {
            byteBufferResponseRecorder.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}