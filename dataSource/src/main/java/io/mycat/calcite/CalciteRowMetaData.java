package io.mycat.calcite;

import io.mycat.beans.mycat.MycatRowMetaData;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.sql.ResultSetMetaData;
import java.util.List;

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