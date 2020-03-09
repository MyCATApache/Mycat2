package io.mycat.hbt;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hbt.ast.query.FieldType;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class HbtRowMetaData implements MycatRowMetaData {
    final List<FieldType> fieldTypeList;

    public HbtRowMetaData(List<FieldType> fieldTypeList) {
        this.fieldTypeList = new ArrayList<>();
        this.fieldTypeList.add(null);
        this.fieldTypeList.addAll(fieldTypeList);
    }

    @Override
    public int getColumnCount() {
        return fieldTypeList.size()-1;
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
        return fieldTypeList.get(column).isNullable();
    }

    @Override
    public boolean isSigned(int column) {
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0;
    }

    @Override
    public String getColumnName(int column) {
        return fieldTypeList.get(column).getId();
    }

    @Override
    public String getSchemaName(int column) {
        return "";
    }

    @Override
    public int getPrecision(int column) {
        return fieldTypeList.get(column).getPrecision();
    }

    @Override
    public int getScale(int column) {
        return fieldTypeList.get(column).getScale();
    }

    @Override
    public String getTableName(int column) {
        return "";
    }

    @Override
    public int getColumnType(int column) {
        String type = fieldTypeList.get(column).getType();
        return Objects.requireNonNull(HBTCalciteSupport.INSTANCE.getSqlTypeName(type)).getJdbcOrdinal();
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