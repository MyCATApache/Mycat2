package io.mycat.hbt4.executor;


import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.function.Predicate;

public class MycatFilterExecutor implements Executor {
    private final Predicate<Row> predicate;
    private final Executor input;

    public MycatFilterExecutor(Predicate<Row> predicate, Executor input) {
        this.predicate = predicate;
        this.input = input;
    }

    @Override
    public void open() {
        input.open();
    }

    @Override
    public Row next() {
        Row row;
        do {
            row = input.next();
            if (row == null) {
                input.close();
                return null;
            }
        } while (predicate.test(row) != Boolean.TRUE);
        return row;
    }

    @Override
    public void close() {
        input.close();
    }
}