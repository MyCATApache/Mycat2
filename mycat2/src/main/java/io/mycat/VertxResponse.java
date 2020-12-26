package io.mycat;

import io.mycat.api.collector.RowIterable;
import io.mycat.resultset.TextResultSetResponse;
import io.vertx.core.buffer.Buffer;

import java.util.Iterator;

public class VertxResponse implements Response {

    private VertxSession session;
    private final int size;
    private int count;

    public VertxResponse(VertxSession session, int size) {
        this.session = session;
        this.size = size;
    }

    @Override
    public void sendError(Throwable e) {

    }

    @Override
    public void proxySelectToPrototype(String statement) {

    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {

    }

    @Override
    public void proxyUpdate(String defaultTargetName, String proxyUpdate) {

    }


    @Override
    public void sendError(String errorMessage, int errorCode) {

    }

    @Override
    public void sendResultSet(RowIterable rowIterable) {
        count++;
        TextResultSetResponse textResultSetResponse = new TextResultSetResponse(rowIterable.get());
        byte[] bytes2 = MySQLPacketUtil.generateResultSetCount(textResultSetResponse.columnCount());
        session.writeBytes(((MySQLPacketUtil.generateMySQLPacket(session.getNextPacketId(), bytes2))),false);
        Iterator<byte[]> iterator = textResultSetResponse.columnDefIterator();
        while (iterator.hasNext()) {
            session.writeBytes((MySQLPacketUtil.generateMySQLPacket(session.getNextPacketId(), iterator.next())),false);
        }
        session.writeBytes((MySQLPacketUtil.generateMySQLPacket(session.getNextPacketId(), MySQLPacketUtil.generateEof(0, 0))),false);
        Iterator<byte[]> iterator1 = textResultSetResponse.rowIterator();
        while (iterator1.hasNext()) {
            session.writeBytes((MySQLPacketUtil.generateMySQLPacket(session.getNextPacketId(), iterator1.next())),false);
        }
        session.writeBytes((MySQLPacketUtil.generateMySQLPacket(session.getNextPacketId(), MySQLPacketUtil.generateEof(0, 0))),true);
    }


    @Override
    public void rollback() {

    }

    @Override
    public void begin() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void execute(ExplainDetail detail) {

    }

    @Override
    public void sendOk(long lastInsertId, long affectedRow) {
        count++;
        MycatDataContext dataContext = session.getDataContext();
        dataContext.setLastInsertId(lastInsertId);
        dataContext.setAffectedRows(affectedRow);
        session.writeOk(count < size);

    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }
}