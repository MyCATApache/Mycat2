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