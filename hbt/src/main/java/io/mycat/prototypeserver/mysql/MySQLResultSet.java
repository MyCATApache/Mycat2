package io.mycat.prototypeserver.mysql;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatMySQLRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.packet.ColumnDefPacket;

import java.util.ArrayList;
import java.util.List;

public class MySQLResultSet {
    List<ColumnDefPacket> columnDefPackets = new ArrayList<>();
    List<Object[]> rows = new ArrayList<>();

    public void addColumn(ColumnDefPacket columnDefPacket) {
        columnDefPackets.add(columnDefPacket);
    }

    public void addRow(Object... row) {
        rows.add((row));
    }

    public static MySQLResultSet create() {
        return new MySQLResultSet();
    }

    public static MySQLResultSet create(List<ColumnDefPacket> columnDefPackets) {
        MySQLResultSet mySQLResultSet = new MySQLResultSet();
        mySQLResultSet.columnDefPackets = columnDefPackets;
        return mySQLResultSet;
    }

    public static MySQLResultSet create(ColumnDefPacket... columnDefPackets) {
        MySQLResultSet mySQLResultSet = new MySQLResultSet();
        for (ColumnDefPacket columnDefPacket : columnDefPackets) {
            mySQLResultSet.addColumn(columnDefPacket);
        }
        return mySQLResultSet;
    }

    public static MySQLResultSet create(ColumnDefPacket[] columnDefPackets, List<Object[]> rows) {
        MySQLResultSet mySQLResultSet = new MySQLResultSet();
        for (ColumnDefPacket columnDefPacket : columnDefPackets) {
            mySQLResultSet.addColumn(columnDefPacket);
        }
        mySQLResultSet.rows = rows;
        return mySQLResultSet;
    }

    public void setRows(List<Object[]> rows) {
        this.rows = rows;
    }

    public RowBaseIterator build() {
        MycatMySQLRowMetaData mycatMySQLRowMetaData = new MycatMySQLRowMetaData(columnDefPackets);
        return new ResultSetBuilder.DefObjectRowIteratorImpl(mycatMySQLRowMetaData, rows.iterator());
    }
}
