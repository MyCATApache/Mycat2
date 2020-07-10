package io.mycat.hbt4.executor;


import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.HashSet;
import java.util.Iterator;

public class MycatUnionDistinctExecutor implements Executor {
    int index = 0;
    final Executor[] executors;
    private Iterator<Row> iterator;

    public MycatUnionDistinctExecutor(Executor[] executors) {
        this.executors = executors;
    }

    @Override
    public void open() {
        for (Executor executor : executors) {
            executor.open();
        }
        HashSet<Row> set = new HashSet<>();
        for (Executor executor : executors) {
            Row row = executor.next();
            if (row == null) {
                executor.close();
            } else {
                set.add(row);
            }
        }
        this.iterator = set.iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }
    }
}