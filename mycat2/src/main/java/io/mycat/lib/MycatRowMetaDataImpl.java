package io.mycat.lib;

import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import io.mycat.beans.mycat.MycatRowMetaData;

import java.sql.ResultSetMetaData;
import java.util.List;

import static java.sql.ResultSetMetaData.columnNullableUnknown;

public class MycatRowMetaDataImpl implements MycatRowMetaData {
    final List<SQLColumnDefinition> columnInfo;
    final String tableName;
    final String schemaName;

    public MycatRowMetaDataImpl(List<SQLColumnDefinition> columnInfo, String schemaName, String tableName) {
        this.columnInfo = columnInfo;
        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    @Override
    public int getColumnCount() {
        return columnInfo.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return columnInfo.get(column).isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) {
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columnInfo.get(column).getColumnName().length();
    }

    @Override
    public String getColumnName(int column) {
        return columnInfo.get(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) {
        return schemaName;
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
        return tableName;
    }

    @Override
    public int getColumnType(int column) {
        return columnInfo.get(column).jdbcType();
    }

    @Override
    public String getColumnLabel(int column) {
        return columnInfo.get(column).getColumnName();
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }
}