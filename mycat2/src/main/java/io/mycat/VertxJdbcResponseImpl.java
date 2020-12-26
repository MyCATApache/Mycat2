package io.mycat;

import io.mycat.api.collector.RowBaseIterator;

public class VertxJdbcResponseImpl extends VertxResponse {
    public VertxJdbcResponseImpl(VertxSession session, int size, boolean binary) {
        super(session, size, binary);
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        MycatConnection connection = transactionSession.getConnection(defaultTargetName);
        sendResultSet(connection.executeQuery(null, statement));
    }

    @Override
    public void proxyUpdate(String defaultTargetName, String proxyUpdate) {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        MycatConnection connection = transactionSession.getConnection(defaultTargetName);
        long[] longs = connection.executeUpdate(null, true);
        transactionSession.closeStatenmentState();
        sendOk(longs[0],longs[1]);
    }

    @Override
    public void rollback() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.rollback();
        session.writeOk(count<size);
    }

    @Override
    public void begin() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.begin();
        session.writeOk(count<size);
    }

    @Override
    public void commit() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.commit();
        session.writeOk(count<size);
    }
}
