package io.mycat.vertx;

import io.mycat.TransactionSession;

public class VertxJdbcResponseImpl extends VertxResponse {
    public VertxJdbcResponseImpl(VertxSession session, int size, boolean binary) {
        super(session, size, binary);
    }


    @Override
    public void rollback() {
        dataContext.getEmitter().onNext(()->{
            count++;
            TransactionSession transactionSession = dataContext.getTransactionSession();
            transactionSession.rollback();
            transactionSession.closeStatenmentState();
            session.writeOk(count<size);
        });
    }

    @Override
    public void begin() {
        dataContext.getEmitter().onNext(()->{
            count++;
            TransactionSession transactionSession = dataContext.getTransactionSession();
            transactionSession.begin();
            session.writeOk(count<size);
        });
    }

    @Override
    public void commit() {
        dataContext.getEmitter().onNext(()->{
            count++;
            TransactionSession transactionSession = dataContext.getTransactionSession();
            transactionSession.commit();
            transactionSession.closeStatenmentState();
            session.writeOk(count<size);
        });
    }


}
