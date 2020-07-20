package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;

public interface  TempResultSetFactory {

    public abstract Executor createRecordSet(String[] fieldNames, Class[] columns);

    public abstract Executor createFixedSizeRecordSet(int size, String[] fieldNames, Class[] columns);

    public abstract Executor makeRewind(Executor executor);
}
