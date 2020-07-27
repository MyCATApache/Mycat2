package io.mycat.hbt4;

import io.mycat.mpp.Row;

import java.util.Objects;

public class AssignExecutor implements Executor {
    final Executor executor;
    final MycatContext context;
    final int[] assignMap;

    public AssignExecutor(Executor input, MycatContext context, int[] assignMap) {
        this.executor = Objects.requireNonNull(input);
        this.context = Objects.requireNonNull(context);
        this.assignMap = Objects.requireNonNull(assignMap);
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
        Object[] values = row.getValues();
        Object[] slots = context.getSlots();
        for (int i = 0; i < assignMap.length; i++) {
            slots[i] = values[assignMap[i]];
        }
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