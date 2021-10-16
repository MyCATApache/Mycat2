/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mycat;

import java.io.Serializable;
import java.sql.ResultSetMetaData;

public class CopyMycatRowMetaData implements MycatRowMetaData, Serializable {
    final Column[] columns;

    public CopyMycatRowMetaData(Column[] columns) {
        this.columns = columns;
    }

    public CopyMycatRowMetaData(MycatRowMetaData mycatRowMetaData) {
        int columnCount = mycatRowMetaData.getColumnCount();
        this.columns = new Column[columnCount];
        for (int i = 0; i < columnCount; i++) {
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
        return columns.length;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return columns[column].isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return columns[column].isCaseSensitive();
    }

    @Override
    public boolean isSigned(int column) {
        return columns[column].isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columns[column].getColumnDisplaySize();
    }

    @Override
    public String getColumnName(int column) {
        return columns[column].getColumnName();
    }

    @Override
    public String getSchemaName(int column) {
        return columns[column].getSchemaName();
    }

    @Override
    public int getPrecision(int column) {
        return columns[column].getPrecision();
    }

    @Override
    public int getScale(int column) {
        return columns[column].getScale();
    }

    @Override
    public String getTableName(int column) {
        return columns[column].getTableName();
    }

    @Override
    public int getColumnType(int column) {
        return columns[column].getColumnType();
    }

    @Override
    public String getColumnLabel(int column) {
        return columns[column].getColumnLabel(column);
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }

    @Override
    public boolean isNullable(int column) {
        return columns[column].isNullable(column);
    }


    public static class Column implements Serializable{
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

        public boolean isCaseSensitive() {
            return caseSensitive;
        }

        public boolean isSigned() {
            return signed;
        }

        public int getColumnDisplaySize() {
            return columnDisplaySize;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public int getPrecision() {
            return precision;
        }

        public int getScale() {
            return scale;
        }

        public String getTableName() {
            return tableName;
        }

        public int getColumnType() {
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