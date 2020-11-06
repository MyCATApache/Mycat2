package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.List;

public class MutilInsertExecutor implements Executor {
    List<MycatInsertExecutor> insertExecutorList;

    public MutilInsertExecutor(List<MycatInsertExecutor> insertExecutorList) {
        this.insertExecutorList = insertExecutorList;
    }
    public static MutilInsertExecutor create(List<MycatInsertExecutor> insertExecutorList){
        return new MutilInsertExecutor(insertExecutorList);
    }

    @Override
    public void open() {
        for (MycatInsertExecutor insertExecutor : insertExecutorList) {
            insertExecutor.open();
        }

    }

    @Override
    public Row next() {
        return null;
    }

    @Override
    public void close() {
        for (MycatInsertExecutor insertExecutor : insertExecutorList) {
            insertExecutor.close();
        }

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}