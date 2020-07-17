package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

public class MycatMergeJoinExecutor implements Executor {
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