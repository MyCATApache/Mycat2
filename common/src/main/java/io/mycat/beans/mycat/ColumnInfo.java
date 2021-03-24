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