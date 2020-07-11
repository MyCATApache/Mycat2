package io.mycat.hbt4;

import io.mycat.hbt3.Part;
import io.mycat.hbt4.executor.ScanExecutor;

public class DatasourceFactoryImpl implements DatasourceFactory {
    @Override
    public Executor create(Part... parts) {
        return new ScanExecutor();
    }

    @Override
    public void close() throws Exception {

    }
}