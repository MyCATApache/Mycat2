package io.mycat.proxy.session.resultSetProcesss;

import io.mycat.proxy.packet.ColumnDefPacket;
import io.mycat.proxy.session.MycatSession;

public class ResultSetImpl extends ResultSet<ColumnDefPacket, Row, MycatSession> {
    public ResultSetImpl(ColumnDefPacket[] columnDefList, RowIterator<Row> rowDataIterator) {
        super(columnDefList, rowDataIterator);
    }

    @Override
    public void writeColunmDefEOfPacket(ColumnDefPacket[] columnDefList, MycatSession mycat) {


    }

    @Override
    public void writeColunmCountPacket(int count, MycatSession mycat) {

    }

    @Override
    public void writeColunmPacket(ColumnDefPacket columnDefPacket, MycatSession mycat) {
        columnDefPacket.writeToChannel(mycat.channel());
    }


    @Override
    public void writeResultRow(Row row, MycatSession mycat) {
     //   row.writeToChannel(mycat.channel());
    }
}
