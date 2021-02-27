/**
 * Copyright (C) <2019>  <chen junwen>
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

import io.mycat.MycatException;
import lombok.SneakyThrows;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static java.sql.DatabaseMetaData.columnNullable;

/**
 * @author Junwen Chen
 **/
public class JdbcRowMetaData implements MycatRowMetaData {

    final ResultSetMetaData resultSetMetaData;
    private final int columnCount;

    @SneakyThrows
    public JdbcRowMetaData(ResultSetMetaData resultSetMetaData) {
        this.resultSetMetaData = resultSetMetaData;
        this.columnCount = this.resultSetMetaData.getColumnCount();
    }

    private String toMessage(Exception e) {
        return e.toString();
    }

    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        try {
            return resultSetMetaData.isAutoIncrement(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean isCaseSensitive(int column) {
        try {
            return resultSetMetaData.isCaseSensitive(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean isNullable(int column) {
        try {
            return resultSetMetaData.isNullable(column+1) == columnNullable;
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean isSigned(int column) {
        try {
            return resultSetMetaData.isSigned(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public int getColumnDisplaySize(int column) {
        try {
            return resultSetMetaData.getColumnDisplaySize(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public String getColumnName(int column) {
        try {
            return resultSetMetaData.getColumnLabel(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public String getSchemaName(int column) {
        try {
            return resultSetMetaData.getSchemaName(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public int getPrecision(int column) {
        try {
            return resultSetMetaData.getPrecision(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public int getScale(int column) {
        try {
            return resultSetMetaData.getScale(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public String getTableName(int column) {
        try {
            return resultSetMetaData.getTableName(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public int getColumnType(int column) {
        try {
            return resultSetMetaData.getColumnType(column+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public String getColumnLabel(int i) {
        try {
            return resultSetMetaData.getColumnLabel(i+1);
        } catch (SQLException e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public ResultSetMetaData metaData() {
        return resultSetMetaData;
    }
}