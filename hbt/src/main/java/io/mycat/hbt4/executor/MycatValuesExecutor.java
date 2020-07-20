package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.Iterator;
import java.util.List;

public class MycatValuesExecutor implements Executor {
    private List<Row> rows;
    private Iterator<Row> iter;

    public MycatValuesExecutor(ImmutableList<Row> rows) {
        this.rows = rows;
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