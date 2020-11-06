package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.Iterator;
import java.util.List;

public class MycatValuesExecutor implements Executor {
    private List<Row> rows;
    private Iterator<Row> iter;

    public MycatValuesExecutor(List<Row> rows) {
        this.rows = rows;
    }

    public static MycatValuesExecutor create(List<Row> rows) {
        return new MycatValuesExecutor(rows);
    }

    @Override
    public void open() {
        this.iter = rows.iterator();
    }

    @Override
    public Row next() {
        if (this.iter.hasNext()) {
            return this.iter.next();
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