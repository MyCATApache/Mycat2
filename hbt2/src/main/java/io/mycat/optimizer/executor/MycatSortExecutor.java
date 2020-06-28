package io.mycat.optimizer.executor;

import io.mycat.mpp.plan.DataAccessor;
import io.mycat.optimizer.Executor;
import io.mycat.optimizer.Row;

import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.StreamSupport;


public class MycatSortExecutor implements Executor {
    private final Executor input;
    private final Comparator<Row> comparator;
    private Iterator<Row> iterator;

    public MycatSortExecutor( Comparator<Row> comparator,Executor input) {
        this.comparator = comparator;
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
        iterator = StreamSupport.stream(iterable.spliterator(), false).sorted(comparator).iterator();
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