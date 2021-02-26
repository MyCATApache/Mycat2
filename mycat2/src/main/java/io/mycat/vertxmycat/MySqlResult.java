package io.mycat.vertxmycat;

import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;

public class MySqlResult<T> implements SqlResult<T> {

    private final long affectedRows;
    private final T result;
    private final long lastInsertId;
    private List<ColumnDescriptor> columnDescriptor;
    private int count;

    public MySqlResult(int count, long affectedRows, long lastInsertId, T result, List<ColumnDescriptor> columnDescriptor) {
        this.count = count;
        this.affectedRows = affectedRows;
        this.result = result;
        this.lastInsertId = lastInsertId;
        this.columnDescriptor = columnDescriptor;
    }

    @Override
    public int rowCount() {
        return (int) affectedRows;
    }

    @Override
    public List<String> columnsNames() {
        return this.columnDescriptor.stream().map(i -> i.name()).collect(Collectors.toList());
    }

    @Override
    public List<ColumnDescriptor> columnDescriptors() {
        return columnDescriptor;
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public T value() {
        return result;
    }

    @Override
    public SqlResult next() {
        return null;
    }

    @Override
    public Object property(PropertyKind propertyKind) {
        if (MySQLClient.LAST_INSERTED_ID == propertyKind) {
            Object lastInsertId1 = lastInsertId;
            return lastInsertId1;
        }
        return null;
    }
}