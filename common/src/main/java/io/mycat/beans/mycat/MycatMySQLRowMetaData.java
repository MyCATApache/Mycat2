package io.mycat.beans.mycat;

import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.vertx.mysqlclient.impl.datatype.DataType;
import lombok.Getter;

import java.sql.ResultSetMetaData;
import java.util.List;

@Getter
public class MycatMySQLRowMetaData implements MycatRowMetaData {
    final List<ColumnDefPacket> columnDefPackets;

    public MycatMySQLRowMetaData(List<ColumnDefPacket> columnDefPackets) {
        this.columnDefPackets = columnDefPackets;
    }

    @Override
    public int getColumnCount() {
        return columnDefPackets.size();
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
        int columnFlags = columnDefPackets.get(column).getColumnFlags();
        boolean unsigned = (columnFlags & MySQLFieldsType.UNSIGNED_FLAG) != 0;
        return !unsigned;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columnDefPackets.get(column).getColumnLength();
    }

    @Override
    public String getColumnName(int column) {
        return columnDefPackets.get(column).getColumnNameString();
    }

    @Override
    public String getSchemaName(int column) {
        return new String(columnDefPackets.get(column).getColumnSchema());
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return columnDefPackets.get(column).getColumnDecimals();
    }

    @Override
    public String getTableName(int column) {
        return new String(columnDefPackets.get(column).getColumnTable());
    }

    @Override
    public int getColumnType(int column) {
        ColumnDefPacket columnDefPacket = columnDefPackets.get(column);
        return DataType.valueOf(columnDefPacket.getColumnType()).jdbcType.getVendorTypeNumber();
    }

    @Override
    public String getColumnLabel(int column) {
        return columnDefPackets.get(column).getColumnNameString();
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }

    @Override
    public boolean isNullable(int column) {
        return (columnDefPackets.get(column).getColumnFlags() & MySQLFieldsType.NOT_NULL_FLAG) == 0;
    }
}
