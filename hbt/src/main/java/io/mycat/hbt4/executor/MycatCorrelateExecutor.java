package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.Enumerable;

import java.util.Iterator;

public class MycatCorrelateExecutor implements Executor {
    private final Iterator<Row> iterator;
    private Enumerable<Row> correlateJoin;

    public MycatCorrelateExecutor(Enumerable<Row> correlateJoin) {
        this.correlateJoin = correlateJoin;
        this.iterator = this.correlateJoin.iterator();
    }

    @Override
    public void open() {

    }

    @Override
    public Row next() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}