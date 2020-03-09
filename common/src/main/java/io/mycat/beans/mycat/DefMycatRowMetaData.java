package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;
import java.util.List;



/**
 * @author Junwen Chen
 **/
public class DefMycatRowMetaData implements MycatRowMetaData {
    final List<ColumnInfo> columnInfos ;

    public DefMycatRowMetaData(List<ColumnInfo> columnInfos) {
        this.columnInfos = columnInfos;
    }

    @Override
    public int getColumnCount() {
        return columnInfos.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return columnInfos.get(column).isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return columnInfos.get(column).isCaseSensitive();
    }

    @Override
    public boolean isNullable(int column) {
        return columnInfos.get(column).isNullable();
    }

    @Override
    public boolean isSigned(int column) {
        return columnInfos.get(column).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columnInfos.get(column).getDisplaySize();
    }

    @Override
    public String getColumnName(int column) {
        return columnInfos.get(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) {
        return columnInfos.get(column).getSchemaName();
    }

    @Override
    public int getPrecision(int column) {
        return columnInfos.get(column).getPrecision();
    }

    @Override
    public int getScale(int column) {
        return columnInfos.get(column).getScale();
    }

    @Override
    public String getTableName(int column) {
        return columnInfos.get(column).getTableName();
    }

    @Override
    public int getColumnType(int column) {
        return columnInfos.get(column).getColumnType();
    }

    @Override
    public String getColumnLabel(int column) {
        return columnInfos.get(column).getColumnLabel();
    }

    @Override
    public ResultSetMetaData metaData() {
       throw new UnsupportedOperationException();
    }
}