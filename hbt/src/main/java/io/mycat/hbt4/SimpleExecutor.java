package io.mycat.hbt4;

import io.mycat.mpp.Row;

import java.util.Iterator;
import java.util.List;

public class SimpleExecutor implements Executor {
    final List<Row> rows;
    private Iterator<Row> iterator;

    public SimpleExecutor(List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public void open() {
        this.iterator = rows.iterator();
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

    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }
}