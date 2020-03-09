package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;
import java.sql.Types;

public class PrepareMycatRowMetaData implements MycatRowMetaData {
    final int columnCount;

    public PrepareMycatRowMetaData(int columnCount) {
        this.columnCount = columnCount;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return false;
    }

    @Override
    public boolean isNullable(int column) {
        return true;
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0;
    }

    @Override
    public String getColumnName(int column) {
        return "?";
    }

    @Override
    public String getSchemaName(int column) {
        return null;
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return null;
    }

    @Override
    public int getColumnType(int column) {
        return Types.NULL;
    }

    @Override
    public String getColumnLabel(int column) {
        return null;
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }
}