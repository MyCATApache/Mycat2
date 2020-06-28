package io.mycat.optimizer;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public interface Executor extends Iterable<Row> {
    public void open();

    public Row next();

    public void close();

    @NotNull
    @Override
    default Iterator<Row> iterator() {
        return new Iterator<Row>() {
            Row row;

            @Override
            public boolean hasNext() {
                row = Executor.this.next();
                return row != null;
            }

            @Override
            public Row next() {
                return row;
            }
        };
    }
}