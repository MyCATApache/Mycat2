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
package io.mycat.calcite.resultset;

import io.mycat.beans.mycat.MycatRowMetaData;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * @author Junwen Chen
 **/
public class CalciteRowMetaData  implements MycatRowMetaData {
    final List<RelDataTypeField> fieldList;

    public CalciteRowMetaData(List<RelDataTypeField> fieldList) {
        this.fieldList = fieldList;
    }

    @Override
    public int getColumnCount() {
        return fieldList.size();
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
    public int isNullable(int column) {
        return getColumn(column).getType().isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }


    @Override
    public boolean isSigned(int column) {
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return -1;
    }

    @Override
    public String getColumnName(int column) {
        return getColumn(column).getName();
    }

    @Override
    public String getSchemaName(int column) {
        return "";
    }

    @Override
    public int getPrecision(int column) {
        return getColumn(column).getType().getPrecision();
    }

    @Override
    public int getScale(int column) {
        return getColumn(column).getType().getScale();
    }

    @Override
    public String getTableName(int column) {
        return "";
    }

    @Override
    public int getColumnType(int column) {
        return getColumn(column).getType().getSqlTypeName().getJdbcOrdinal();
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumnName(column);
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }
    private RelDataTypeField getColumn(int column) {
        return fieldList.get(column-1);
    }
};