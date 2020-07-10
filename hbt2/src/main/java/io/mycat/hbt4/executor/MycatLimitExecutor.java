package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.Iterator;
import java.util.stream.StreamSupport;


public class MycatLimitExecutor implements Executor {
    private final Executor input;
    private long offset;
    private long fetch;
    private Iterator<Row> iterator;

    public MycatLimitExecutor(long offset,long fetch, Executor input) {
        this.offset = offset;
        this.fetch = fetch;
        this.input = input;
    }

    @Override
    public void open() {
        input.open();
        Iterable<Row> iterable = () -> new Iterator<Row>() {
            Row row = null;

            @Override
            public boolean hasNext() {
                row = input.next();
                return row != null;
            }

            @Override
            public Row next() {
                return row;
            }
        };
      this.iterator = StreamSupport.stream(iterable.spliterator(), false).skip(offset).limit(fetch).iterator();
    }

    @Override
    public Row next() {
        if(iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        input.close();
    }
}