package io.mycat.vertx;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.DirectTextResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;

import java.util.Iterator;
import java.util.Objects;

import static io.mycat.ExecuteType.QUERY;
import static io.mycat.ExecuteType.UPDATE;

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
    public void proxySelect(String defaultTargetName, String statement) {
        execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }

    @Override
    public void proxyUpdate(String defaultTargetName, String proxyUpdate) {
        execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(defaultTargetName), proxyUpdate, null));
    }

    @Override
    public void sendError(Throwable e) {
        dataContext.getEmitter().onNext(()->{
            dataContext.getTransactionSession().closeStatenmentState();
            dataContext.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
        });
    }

    @Override
    public void proxySelectToPrototype(String statement) {
        proxySelect("prototype",statement);
    }


    @Override
    public void sendError(String errorMessage, int errorCode) {
        dataContext.getEmitter().onNext(()->{
            dataContext.getTransactionSession().closeStatenmentState();
            dataContext.setLastMessage(errorMessage);
            session.writeErrorEndPacketBySyncInProcessError();
        });
    }

    @Override
    public void sendResultSet(RowIterable rowIterable) {
        dataContext.getEmitter().onNext(()->{
            ++count;
            RowBaseIterator resultSet = rowIterable.get();
            boolean moreResultSet = count < size;
            MycatResultSetResponse currentResultSet;
            if (!binary) {
                if (resultSet instanceof JdbcRowBaseIterator){
                    currentResultSet = new DirectTextResultSetResponse((resultSet));
                }else {
                    currentResultSet = new TextResultSetResponse(resultSet);
                }
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
        });
    }

    @Override
    public void execute(ExplainDetail detail) {
        dataContext.getEmitter().onNext(()->{
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
            TransactionSession transactionSession = dataContext.getTransactionSession();
            MycatConnection connection = transactionSession.getConnection(target);
            count++;
            switch (executeType) {
                case QUERY:
                case QUERY_MASTER:
                    sendResultSet(connection.executeQuery(null, sql));
                    break;
                case INSERT:
                case UPDATE:
                    long[] longs = connection.executeUpdate(sql, true);
                    transactionSession.closeStatenmentState();
                    sendOk(longs[0],longs[1]);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + executeType);
            }
        });
    }

    @Override
    public void sendOk(long affectedRow,long lastInsertId ) {
        dataContext.getEmitter().onNext(()->{
            count++;
            MycatDataContext dataContext = session.getDataContext();
            dataContext.getTransactionSession().closeStatenmentState();
            dataContext.setLastInsertId(lastInsertId);
            dataContext.setAffectedRows(affectedRow);
            session.writeOk(count < size);
        });
    }

    @Override
    public void sendOk() {
        dataContext.getEmitter().onNext(()->{
            count++;
            MycatDataContext dataContext = session.getDataContext();
            dataContext.getTransactionSession().closeStatenmentState();
            session.writeOk(count < size);
        });
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }
}