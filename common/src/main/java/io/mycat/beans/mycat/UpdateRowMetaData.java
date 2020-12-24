package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;
import java.sql.Types;

public enum UpdateRowMetaData implements MycatRowMetaData {
    INSTANCE;
    public static final String UPDATE_COUNT = "UPDATE_COUNT";
    public static final String LAST_INSERT_ID = "LAST_INSERT_ID";


    @Override
    public int getColumnCount() {
        return 2;
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
        switch (column) {
            case 1:
                return UPDATE_COUNT;
            case 2:
                return LAST_INSERT_ID;
        }
        throw new IllegalArgumentException();
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
        switch (column) {
            case 1:
                return Types.BIGINT;
            case 2:
                return Types.BIGINT;
        }
        return 0;
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumnName(column);
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }
}