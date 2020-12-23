package io.mycat.beans.mycat;

import java.io.Serializable;
import java.sql.ResultSetMetaData;

public class CopyMycatRowMetaData implements MycatRowMetaData, Serializable {
    final Column[] columns;

    public CopyMycatRowMetaData(MycatRowMetaData mycatRowMetaData) {
        int columnCount = mycatRowMetaData.getColumnCount();
        this.columns = new Column[columnCount + 1];
        for (int i = 1; i <= columnCount; i++) {
            boolean autoIncrement = mycatRowMetaData.isAutoIncrement(i);
            boolean caseSensitive = mycatRowMetaData.isCaseSensitive(i);
            boolean signed = mycatRowMetaData.isSigned(i);
            int columnDisplaySize = mycatRowMetaData.getColumnDisplaySize(i);
            String columnName = mycatRowMetaData.getColumnName(i);
            String schemaName = mycatRowMetaData.getSchemaName(i);
            int precision = mycatRowMetaData.getPrecision(i);
            int scale = mycatRowMetaData.getScale(i);
            String tableName = mycatRowMetaData.getTableName(i);
            int columnType = mycatRowMetaData.getColumnType(i);
            String columnLabel = mycatRowMetaData.getColumnLabel(i);
            boolean nullable = mycatRowMetaData.isNullable(i);
            columns[i] = new Column(autoIncrement,
                    caseSensitive,
                    signed,
                    columnDisplaySize,
                    columnName,
                    schemaName,
                    precision,
                    scale,
                    tableName,
                    columnType,
                    columnLabel,
                    nullable
            );
        }
    }


    @Override
    public int getColumnCount() {
        return columns.length - 1;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return columns[column].isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return columns[column].isCaseSensitive(column);
    }

    @Override
    public boolean isSigned(int column) {
        return columns[column].isSigned(column);
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columns[column].getColumnDisplaySize(column);
    }

    @Override
    public String getColumnName(int column) {
        return columns[column].getColumnName(column);
    }

    @Override
    public String getSchemaName(int column) {
        return columns[column].getSchemaName(column);
    }

    @Override
    public int getPrecision(int column) {
        return columns[column].getPrecision(column);
    }

    @Override
    public int getScale(int column) {
        return columns[column].getScale(column);
    }

    @Override
    public String getTableName(int column) {
        return columns[column].getTableName(column);
    }

    @Override
    public int getColumnType(int column) {
        return columns[column].getColumnType(column);
    }

    @Override
    public String getColumnLabel(int column) {
        return columns[column].getColumnLabel(column);
    }

    @Override
    public ResultSetMetaData metaData() {
        throw new AssertionError();
    }

    @Override
    public boolean isNullable(int column) {
        return columns[column].isNullable(column);
    }


    public static class Column {
        boolean autoIncrement;
        boolean caseSensitive;
        boolean signed;
        int columnDisplaySize;
        String columnName;
        String schemaName;
        int precision;
        int scale;
        String tableName;
        int columnType;
        String columnLabel;
        boolean nullable;

        public Column(boolean autoIncrement,
                      boolean caseSensitive,
                      boolean signed,
                      int columnDisplaySize,
                      String columnName,
                      String schemaName,
                      int precision,
                      int scale,
                      String tableName,
                      int columnType,
                      String columnLabel,
                      boolean nullable) {
            this.autoIncrement = autoIncrement;
            this.caseSensitive = caseSensitive;
            this.signed = signed;
            this.columnDisplaySize = columnDisplaySize;
            this.columnName = columnName;
            this.schemaName = schemaName;
            this.precision = precision;
            this.scale = scale;
            this.tableName = tableName;
            this.columnType = columnType;
            this.columnLabel = columnLabel;
            this.nullable = nullable;
        }

        public boolean isAutoIncrement() {
            return autoIncrement;
        }

        public boolean isCaseSensitive(int column) {
            return caseSensitive;
        }

        public boolean isSigned(int column) {
            return signed;
        }

        public int getColumnDisplaySize(int column) {
            return columnDisplaySize;
        }

        public String getColumnName(int column) {
            return columnName;
        }

        public String getSchemaName(int column) {
            return schemaName;
        }

        public int getPrecision(int column) {
            return precision;
        }

        public int getScale(int column) {
            return scale;
        }

        public String getTableName(int column) {
            return tableName;
        }

        public int getColumnType(int column) {
            return columnType;
        }

        public String getColumnLabel(int column) {
            return columnLabel;
        }

        public boolean isNullable(int column) {
            return nullable;
        }
    }
}