package io.mycat.hbt4;

import io.mycat.hbt4.executor.ScanExecutor;

public class DatasourceFactoryImpl implements DatasourceFactory {


    @Override
    public void close() {

    }


    @Override
    public Executor create(int index, String sql, Object[] objects) {
        return new ScanExecutor();
    }

    @Override
    public void createTableIfNotExisted(int index, String createTableSql) {

    }
}