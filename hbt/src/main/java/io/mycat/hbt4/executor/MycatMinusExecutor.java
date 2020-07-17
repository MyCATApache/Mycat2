package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.Iterator;

public class MycatMinusExecutor implements Executor {
    private Executor[] executors;
    private boolean all;
    private Iterator<Row> iterator;
    private Enumerable<Row> enumerables;

    public MycatMinusExecutor(Executor[] executors, boolean all) {
        this.executors = executors;
        this.all = all;
    }


    @Override
    public void open() {
        if (enumerables == null) {
            for (Executor executor : executors) {
                executor.open();
            }
            Enumerable<Row> acc = Linq4j.emptyEnumerable();
            for (Executor i : executors) {
                Enumerable<Row> rows = Linq4j.asEnumerable(i);
                acc = acc.except(rows, all);
            }
            this.enumerables = acc;
            for (Executor executor : executors) {
                executor.close();
            }
        }
        this.iterator = this.enumerables.iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }
    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }
};