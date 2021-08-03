package io.mycat.connection;

import io.mycat.beans.mycat.MycatRowMetaData;
import org.apache.calcite.adapter.jdbc.JdbcTable;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MycatResultSetMetaData implements ResultSetMetaData {
    public final MycatRowMetaData rowMetaData;

    public MycatResultSetMetaData(MycatRowMetaData rowMetaData) {
        this.rowMetaData = rowMetaData;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return rowMetaData.getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return rowMetaData.isAutoIncrement(column - 1);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return rowMetaData.isCaseSensitive(column - 1);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        boolean nullable = rowMetaData.isNullable(column - 1);
        return nullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return rowMetaData.getColumnDisplaySize(column-1);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return rowMetaData.getColumnLabel(column-1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return rowMetaData.getColumnName(column-1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return rowMetaData.getSchemaName(column-1);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return rowMetaData.getPrecision(column-1);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return rowMetaData.getScale(column-1);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return rowMetaData.getTableName(column-1);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "def";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return rowMetaData.getColumnType(column-1);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
       return JDBCType.valueOf( getColumnTypeName(column)).getName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return JDBCType.valueOf( getColumnTypeName(column)).getClass().getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
