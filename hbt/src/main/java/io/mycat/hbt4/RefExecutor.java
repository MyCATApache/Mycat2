package io.mycat.hbt4;

import io.mycat.mpp.Row;

public class RefExecutor implements Executor  {
    final Executor input;

    public RefExecutor(Executor input) {
        this.input = input;
    }

    @Override
    public void open() {
        input.open();
    }

    @Override
    public Row next() {
        return input.next();
    }

    @Override
    public void close() {
        input.close();
    }

    @Override
    public boolean isRewindSupported() {
        return input.isRewindSupported();
    }
}