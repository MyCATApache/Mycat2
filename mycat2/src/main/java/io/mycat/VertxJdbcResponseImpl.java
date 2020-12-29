package io.mycat;

import io.mycat.api.collector.RowBaseIterator;

public class VertxJdbcResponseImpl extends VertxResponse {
    public VertxJdbcResponseImpl(VertxSession session, int size, boolean binary) {
        super(session, size, binary);
    }


    @Override
    public void rollback() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.rollback();
        transactionSession.closeStatenmentState();
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
        transactionSession.closeStatenmentState();
        session.writeOk(count<size);
    }
}
