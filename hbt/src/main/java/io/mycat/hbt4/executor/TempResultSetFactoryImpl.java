package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.hbt4.SimpleExecutor;
import org.apache.calcite.linq4j.Linq4j;

public class TempResultSetFactoryImpl implements TempResultSetFactory {
    @Override
    public Executor createRecordSet(String[] fieldNames, Class[] columns) {
        return null;
    }

    @Override
    public Executor createFixedSizeRecordSet(int size, String[] fieldNames, Class[] columns) {
        return null;
    }

    public Executor makeRewind(Executor executor) {
        return new SimpleExecutor(Linq4j.asEnumerable(executor).toList());
    }
}