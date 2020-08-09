package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.hbt4.MycatContext;
import io.mycat.mpp.Row;

import java.util.Objects;

public class AssignExecutor implements Executor {
    final Executor executor;
    final MycatContext context;

    public AssignExecutor(Executor input, MycatContext context) {
        this.executor = Objects.requireNonNull(input);
        this.context = Objects.requireNonNull(context);
    }

    public static AssignExecutor create(Executor input, MycatContext context) {
        return new AssignExecutor(input,context);
    }

    @Override
    public void open() {
        executor.open();
    }

    @Override
    public Row next() {
        Row row = executor.next();
        if (row == null) {
            return null;
        }
        context.slots = row.getValues();
        return row;
    }

    @Override
    public void close() {
        executor.close();
    }

    @Override
    public boolean isRewindSupported() {
        return executor.isRewindSupported();
    }
}