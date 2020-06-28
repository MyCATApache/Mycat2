package io.mycat.optimizer.executor;

import io.mycat.optimizer.Executor;
import io.mycat.optimizer.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

public class MycatJoinExecutor implements Executor {
    private final Executor[] executors;
    private Enumerable<Row> rows;
    private  Enumerator<Row> enumerator;

    public MycatJoinExecutor(Executor[] executors, Enumerable<Row> rows) {
        this.executors = executors;
        this.rows = rows;
    }

    @Override
    public void open() {
        for (Executor executor : executors) {
            executor.open();
        }
        this.enumerator = rows.enumerator();
    }

    @Override
    public Row next() {
        if(enumerator.moveNext()){
            return enumerator.current();
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