package io.mycat.vertxmycat;

import io.mycat.MycatException;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.session.MySQLClientSession;
import io.netty.buffer.ByteBuf;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.mysqlclient.impl.datatype.DataFormat;
import io.vertx.mysqlclient.impl.datatype.DataType;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.mysqlclient.impl.util.BufferUtils;
import io.vertx.sqlclient.Row;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collector;

public class VertxMycatTextCollector<C, R> implements ResultSetHandler {

    private int columnCount;
    private ColumnDefinition[] currentColumnDefList;
    private MycatVertxRowResultDecoder rowResultDecoder;
    private Collector<Row, C, R> collector;
    private BiConsumer<C, Row> accumulator;
    private C c;
    private R res;
    private int rowCount = 0;
    private long affectedRows;
    private long lastInsertId;
    private int serverStatusFlags;

    public VertxMycatTextCollector(Collector<Row, C, R> collector) {
        this.collector = collector;
        if (!(this.collector instanceof StreamMysqlCollector)){
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {
        ColumnDefPacketImpl packet = new ColumnDefPacketImpl();
        packet.read(mySQLPacket, startPos, endPos);
        String catalog = new String(packet.getColumnCatalog());
        String schema = new String(packet.getColumnSchema());
        String table = new String(packet.getColumnTable());
        String orgTable = new String(packet.getColumnOrgTable());
        String name = new String(packet.getColumnName());
        String orgName = new String(packet.getColumnOrgName());
        int characterSet = packet.getColumnCharsetSet();
        long columnLength = packet.getColumnLength();
        DataType type = DataType.valueOf(packet.getColumnType());
        int flags = packet.getColumnFlags();
        byte decimals = packet.getColumnDecimals();
        ColumnDefinition columnDefinition = new ColumnDefinition(
                catalog,
                schema,
                table,
                orgTable,
                name,
                orgName,
                characterSet,
                columnLength,
                type,
                flags,
                decimals
        );
        this.currentColumnDefList[this.columnCount++] = columnDefinition;

    }

    @Override
    public void onColumnDefEof(MySQLPacket mySQLPacket, int startPos, int endPos) {
        rowResultDecoder = new MycatVertxRowResultDecoder(collector, new MySQLRowDesc(currentColumnDefList, DataFormat.TEXT));
        this.c = collector.supplier().get();
        this.accumulator = collector.accumulator();
        if (collector instanceof StreamMysqlCollector){
            MySQLRowDesc mySQLRowDesc = new MySQLRowDesc(this.currentColumnDefList, DataFormat.TEXT);
            ((StreamMysqlCollector) collector).onColumnDefinitions(mySQLRowDesc,null);
        }
    }

    @Override
    public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) throws MycatException {
        Row row = rowResultDecoder.decodeRow(currentColumnDefList.length, Buffer.buffer(mySQLPacket.getBytes(startPos, endPos-startPos)).getByteBuf());
        rowCount++;
        this.accumulator.accept(this.c, row);
    }

    @Override
    public void onColumnCount(int columnCount) {
        this.columnCount = 0;
        this.currentColumnDefList = new ColumnDefinition[columnCount];
    }

    @Override
    public void onRowOk(MySQLPacket mySQLPacket, int startPos, int endPos) {
        ByteBuf payload = Buffer.buffer(mySQLPacket.getBytes(startPos, endPos-startPos)).getByteBuf();
        payload.skipBytes(1); // skip OK packet header
        this.affectedRows = BufferUtils.readLengthEncodedInteger(payload);
        this.lastInsertId = BufferUtils.readLengthEncodedInteger(payload);
        this.serverStatusFlags = payload.readUnsignedShortLE();
        this.res = collector.finisher().apply(c);
    }

    public MycatVertxRowResultDecoder getRowResultDecoder() {
        return rowResultDecoder;
    }

    public R getRes() {
        return res;
    }

    @Override
    public void onOk(MySQLPacket mySQLPacket, int startPos, int endPos) {
        ByteBuf payload = Buffer.buffer(mySQLPacket.getBytes(startPos, endPos-startPos)).getByteBuf();
        payload.skipBytes(1); // skip OK packet header
        this.affectedRows = BufferUtils.readLengthEncodedInteger(payload);
        this.lastInsertId = BufferUtils.readLengthEncodedInteger(payload);
        this.serverStatusFlags = payload.readUnsignedShortLE();
        if (collector instanceof StreamMysqlCollector){
//            MySQLRowDesc mySQLRowDesc = new MySQLRowDesc(this.currentColumnDefList, DataFormat.TEXT);
            ((StreamMysqlCollector) collector).onFinish(0,serverStatusFlags,affectedRows,lastInsertId);
        }
    }

    @Override
    public void onFinishedCollect(MySQLClientSession mysql) {

    }

    public int getRowCount() {
        return rowCount;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public int getServerStatusFlags() {
        return serverStatusFlags;
    }

    public Collector<Row, C, R> getCollector() {
        return collector;
    }
}
