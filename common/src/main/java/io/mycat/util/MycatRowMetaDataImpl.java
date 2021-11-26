package io.mycat.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.beans.mycat.MycatRowMetaData;

import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Optional;

public class MycatRowMetaDataImpl implements MycatRowMetaData {
    final List<SQLColumnDefinition> columnInfo;
    final List<MySqlTableIndex> indexList;
    final String tableName;
    final String schemaName;
    final int columnCount;
    private MySqlCreateTableStatement mySqlCreateTableStatement;


    public MycatRowMetaDataImpl(MySqlCreateTableStatement mySqlCreateTableStatement) {
        this.mySqlCreateTableStatement = mySqlCreateTableStatement;

        this.columnInfo = this.mySqlCreateTableStatement.getColumnDefinitions();
        this.indexList = this.mySqlCreateTableStatement.getMysqlIndexes();
        this.tableName = SQLUtils.normalize(this.mySqlCreateTableStatement.getTableName());
        this.schemaName = Optional.ofNullable(this.mySqlCreateTableStatement.getSchema()).map(n -> SQLUtils.normalize(n)).orElse(null);
        this.columnCount = columnInfo.size();
    }

    @Override
    public int getColumnCount() {
        return columnCount;
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
    public boolean isNullable(int column) {
        return !columnInfo.get(column).containsNotNullConstaint();
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
        return columnInfo.get(column).computeAlias();
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

    @Override
    public boolean isPrimaryKey(int column) {
        return columnInfo.get(column).isPrimaryKey();
    }

    @Override
    public boolean isUniqueKey(int column) {
        return isPrimaryKey(column) || mySqlCreateTableStatement.isUNI(columnInfo.get(column).getColumnName());
    }
}