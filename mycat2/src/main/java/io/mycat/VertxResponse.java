package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;

import java.util.Iterator;

public abstract class VertxResponse implements Response {

    protected final MycatDataContext dataContext;
    protected VertxSession session;
    protected final int size;
    protected int count;
    protected boolean binary;

    public VertxResponse(VertxSession session, int size, boolean binary) {
        this.session = session;
        this.size = size;
        this.binary = binary;
        this.dataContext = session.getDataContext();
    }

    @Override
    public void sendError(Throwable e) {
        dataContext.setLastMessage(e);
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void proxySelectToPrototype(String statement) {
        proxySelect("prototype",statement);
    }


    @Override
    public void sendError(String errorMessage, int errorCode) {
        dataContext.setLastMessage(errorMessage);
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void sendResultSet(RowIterable rowIterable) {
        ++count;
        RowBaseIterator resultSet = rowIterable.get();
        boolean moreResultSet = count < size;
        MycatResultSetResponse currentResultSet;
        if (!binary) {
            currentResultSet = new TextResultSetResponse(resultSet);
        } else {
            currentResultSet = new BinaryResultSetResponse(resultSet);
        }
        session.writeColumnCount(currentResultSet.columnCount());
        Iterator<byte[]> columnDefPayloadsIterator = currentResultSet
                .columnDefIterator();
        while (columnDefPayloadsIterator.hasNext()) {
            session.writeBytes(columnDefPayloadsIterator.next(), false);
        }
        session.writeColumnEndPacket();
        Iterator<byte[]> rowIterator = currentResultSet.rowIterator();
        while (rowIterator.hasNext()) {
            byte[] row = rowIterator.next();
            session.writeBytes(row, false);
        }
        currentResultSet.close();
        session.getDataContext().getTransactionSession().closeStatenmentState();
        session.writeRowEndPacket(moreResultSet, false);
    }

    @Override
    public void execute(ExplainDetail detail) {
        String target = detail.getTarget();
        ExecuteType executeType = detail.getExecuteType();
        String sql = detail.getSql();
        MycatDataContext dataContext = session.getDataContext();
        switch (executeType) {
            case QUERY:
                target = dataContext.resolveDatasourceTargetName(target, false);
                break;
            case QUERY_MASTER:
            case INSERT:
            case UPDATE:
            default:
                target = dataContext.resolveDatasourceTargetName(target, true);
                break;
        }
        switch (executeType) {
            case QUERY:
            case QUERY_MASTER:
                proxySelect(target,sql);
                break;
            case INSERT:
            case UPDATE:
                proxyUpdate(target,sql);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + executeType);
        }
    }

    @Override
    public void sendOk(long affectedRow,long lastInsertId ) {
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