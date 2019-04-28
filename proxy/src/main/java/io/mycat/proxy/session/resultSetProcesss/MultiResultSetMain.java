package io.mycat.proxy.session.resultSetProcesss;

import io.mycat.proxy.packet.ColumnDefPacket;

import java.util.ArrayList;
import java.util.List;

public class MultiResultSetMain {
    public static void main(String[] args) {
        ColumnDefPacket[] columnDefPackets = {};
        RowIterator<Row> rowDataIterator = new RowIterator<>();
        List<ResultSet> resultSets = new ArrayList<>();
        ResultSetImpl resultSet = new ResultSetImpl(columnDefPackets, rowDataIterator);
        resultSets.add(resultSet);
        MultiResultSet multiResultSet = new MultiResultSetImpl(resultSets.iterator());
        multiResultSet.load();
        while (!multiResultSet.isFinished()){
            multiResultSet.run(null);
        }
    }
}
