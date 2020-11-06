package io.mycat.beans.mycat;

import lombok.Getter;


@Getter
public class ColumnInfo {
    private final String schemaName;
    private final String tableName;
    private final String columnName;
    private final int columnType;
    private final int precision;
    private final int scale;
    private final String columnLabel;
    private final boolean isAutoIncrement;
    private final boolean isCaseSensitive;
    private final boolean isNullable;
    private final boolean isSigned;
    private final int displaySize;

    public ColumnInfo(String schemaName, String tableName, String columnName, int columnType, int precision, int scale, String columnLabel, boolean isAutoIncrement, boolean isCaseSensitive, boolean isNullable, boolean isSigned, int displaySize) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.precision = precision;
        this.scale = scale;
        this.columnLabel = columnLabel;
        this.isAutoIncrement = isAutoIncrement;
        this.isCaseSensitive = isCaseSensitive;
        this.isNullable = isNullable;
        this.isSigned = isSigned;
        this.displaySize = displaySize;
    }

    public ColumnInfo(String tableName, String columnName, int columnType, int precision, int scale) {
        this(tableName, tableName, columnName, columnType, precision, scale, columnName, false, true, true, true, columnName.length());
    }

    public ColumnInfo(String columnName, int columnType) {
        this("", columnName, columnType, 0, 0);
    }

}