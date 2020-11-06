package io.mycat.beans.mycat;

import io.mycat.api.collector.DefObjectRowIteratorImpl;
import io.mycat.api.collector.RowBaseIterator;

import java.util.ArrayList;
import java.util.List;

public class ResultSetBuilder {
    final List<ColumnInfo> columnInfos = new ArrayList<>();
    final List<Object[]> objectList = new ArrayList<>();

    public static ResultSetBuilder create(){
        return new ResultSetBuilder();
    }

    public ResultSetBuilder() {
        columnInfos.add(null);
    }

    public void addColumnInfo(String schemaName, String tableName, String columnName, int columnType, int precision, int scale, String columnLabel, boolean isAutoIncrement, boolean isCaseSensitive, boolean isNullable, boolean isSigned, int displaySize) {
        columnInfos.add(new ColumnInfo(schemaName, tableName, columnName, columnType, precision, scale, columnLabel, isAutoIncrement, isCaseSensitive, isNullable, isSigned, displaySize));
    }

    public void addColumnInfo(String tableName, String columnName, int columnType, int precision, int scale) {
        columnInfos.add(new ColumnInfo(tableName, tableName, columnName, columnType, precision, scale, columnName, false, true, true, true, columnName.length()));
    }

    public void addColumnInfo(String columnName, int columnType) {
        columnInfos.add(new ColumnInfo(columnName, columnType));
    }


    public int columnCount() {
        return columnInfos.size();
    }


    public void addObjectRowPayload(Object[]... row) {
        objectList.add(row);
    }

    public void addObjectRowPayload(List<Object> row) {
        objectList.add(row.toArray());
    }
    public RowBaseIterator build() {
        DefMycatRowMetaData mycatRowMetaData = new DefMycatRowMetaData(columnInfos);
        return new DefObjectRowIteratorImpl(mycatRowMetaData, objectList.listIterator());
    }
}