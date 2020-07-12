package io.mycat.hbt4;

public interface DatasourceFactory extends AutoCloseable {

    Executor create(int index, String sql, Object[] objects);

    public void createTableIfNotExisted(int index, String createTableSql);
}