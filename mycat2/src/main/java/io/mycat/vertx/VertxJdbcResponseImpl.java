package io.mycat.vertx;

import io.mycat.TransactionSession;
import io.vertx.core.impl.future.PromiseInternal;

public class VertxJdbcResponseImpl extends VertxResponse {
    public VertxJdbcResponseImpl(VertxSession session, int size, boolean binary) {
        super(session, size, binary);
    }


    @Override
    public PromiseInternal<Void>  rollback() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.rollback();
        transactionSession.closeStatenmentState();
        return session.writeOk(count<size);
    }

    @Override
    public PromiseInternal<Void>  begin() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.begin();
        return session.writeOk(count<size);
    }

    @Override
    public PromiseInternal<Void> commit() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.commit();
        transactionSession.closeStatenmentState();
        return session.writeOk(count<size);
    }


}
