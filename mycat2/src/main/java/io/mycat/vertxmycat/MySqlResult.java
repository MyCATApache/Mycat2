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
package io.mycat.vertxmycat;

import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;

public class MySqlResult<T> implements SqlResult<T> {

    private final long affectedRows;
    private final T result;
    private final long lastInsertId;
    private List<ColumnDescriptor> columnDescriptor;
    private int count;

    public MySqlResult(int count, long affectedRows, long lastInsertId, T result, List<ColumnDescriptor> columnDescriptor) {
        this.count = count;
        this.affectedRows = affectedRows;
        this.result = result;
        this.lastInsertId = lastInsertId;
        this.columnDescriptor = columnDescriptor;
    }

    @Override
    public int rowCount() {
        return (int) affectedRows;
    }

    @Override
    public List<String> columnsNames() {
        return this.columnDescriptor.stream().map(i -> i.name()).collect(Collectors.toList());
    }

    @Override
    public List<ColumnDescriptor> columnDescriptors() {
        return columnDescriptor;
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public T value() {
        return result;
    }

    @Override
    public SqlResult next() {
        return null;
    }

    @Override
    public Object property(PropertyKind propertyKind) {
        if (MySQLClient.LAST_INSERTED_ID == propertyKind) {
            Object lastInsertId1 = lastInsertId;
            return lastInsertId1;
        }
        return null;
    }
}