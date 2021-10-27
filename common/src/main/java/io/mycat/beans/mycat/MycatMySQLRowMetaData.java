package io.mycat.beans.mycat;

import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.vertx.mysqlclient.impl.datatype.DataType;

import java.sql.ResultSetMetaData;

public class MycatMySQLRowMetaData implements MycatRowMetaData {
    final ColumnDefPacket[] columnDefPackets;

    public MycatMySQLRowMetaData(ColumnDefPacket[] columnDefPackets) {
        this.columnDefPackets = columnDefPackets;
    }

    @Override
    public int getColumnCount() {
        return columnDefPackets.length;
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
    public boolean isSigned(int column) {
        int columnFlags = columnDefPackets[column].getColumnFlags();
        boolean unsigned = (columnFlags & MySQLFieldsType.UNSIGNED_FLAG) != 0;
        return !unsigned;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columnDefPackets[column].getColumnLength();
    }

    @Override
    public String getColumnName(int column) {
        return columnDefPackets[column].getColumnNameString();
    }

    @Override
    public String getSchemaName(int column) {
        return new String(columnDefPackets[column].getColumnSchema());
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return columnDefPackets[column].getColumnDecimals();
    }

    @Override
    public String getTableName(int column) {
        return new String(columnDefPackets[column].getColumnTable());
    }

    @Override
    public int getColumnType(int column) {
        ColumnDefPacket columnDefPacket = columnDefPackets[column];
        return DataType.valueOf(columnDefPacket.getColumnType()).jdbcType.getVendorTypeNumber();
    }

    @Override
    public String getColumnLabel(int column) {
        return columnDefPackets[column].getColumnNameString();
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }

    @Override
    public boolean isNullable(int column) {
        return (columnDefPackets[column].getColumnFlags() & MySQLFieldsType.NOT_NULL_FLAG) == 0;
    }
}
