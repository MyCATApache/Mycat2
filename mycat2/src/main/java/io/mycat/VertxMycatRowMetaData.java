package io.mycat;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.sql.ResultSetMetaData;
import java.util.List;

public class VertxMycatRowMetaData implements MycatRowMetaData {
    public VertxMycatRowMetaData(List<ColumnDescriptor> columnDescriptors) {
        this.columnDescriptors = columnDescriptors;
    }

    final List<ColumnDescriptor> columnDescriptors;
    @Override
    public int getColumnCount() {
        return     this.columnDescriptors.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return  false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return false;
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getColumnName(int column) {
        return columnDescriptors.get(column-1).name();
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
        return columnDescriptors.get(column-1).jdbcType().getVendorTypeNumber();
    }

    @Override
    public String getColumnLabel(int column) {
        return getColumnName(column);
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }

    @Override
    public boolean isNullable(int column) {
        return true;
    }
}
