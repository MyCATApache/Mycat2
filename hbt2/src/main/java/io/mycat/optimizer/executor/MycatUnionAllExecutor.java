package io.mycat.optimizer.executor;

import io.mycat.optimizer.Executor;
import io.mycat.optimizer.Row;

public class MycatUnionAllExecutor implements Executor {
    int index = 0;
    final Executor[] executors;

    public MycatUnionAllExecutor(Executor[] executors) {
        this.executors = executors;
    }

    @Override
    public void open() {
        for (Executor executor : executors) {
            executor.open();
        }
    }

    @Override
    public Row next() {
        Executor executor = executors[index];
        Row row = executor.next();
        if (row == null) {
            executor.close();
            index++;
            if (index >= executors.length) {
                return null;
            } else {
                return next();
            }
        }
        return row;
    }

    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }
    }
}