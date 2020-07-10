package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

public class MycatProjectExecutor implements Executor {
    private MycatScalar mycatScalar;
    private Executor executor;

    public MycatProjectExecutor(MycatScalar mycatScalar, Executor executor) {
        this.mycatScalar = mycatScalar;
        this.executor = executor;
    }

    @Override
    public void open() {
        executor.open();
    }

    @Override
    public Row next() {
        Row next = executor.next();
        if (next == null){
            return null;
        }
        mycatScalar.execute(next,next);
        return next;
    }

    @Override
    public void close() {
        executor.close();
    }
}