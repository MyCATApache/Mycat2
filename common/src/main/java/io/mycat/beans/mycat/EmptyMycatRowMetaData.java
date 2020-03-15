package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;

/**
 * @author jamie12221
 * date 2020-02-26 19:18
 * column information,like a jdbc
 **/
public enum EmptyMycatRowMetaData implements MycatRowMetaData {
    INSTANCE;

    @Override
    public int getColumnCount() {
        return 0;
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
        return false;
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
        return null;
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
        return 0;
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