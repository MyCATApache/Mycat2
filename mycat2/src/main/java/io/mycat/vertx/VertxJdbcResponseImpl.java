package io.mycat.vertx;

import io.mycat.TransactionSession;
import io.mycat.util.VertxUtil;
import io.vertx.core.impl.future.PromiseInternal;

public class VertxJdbcResponseImpl extends VertxResponse {
    public VertxJdbcResponseImpl(VertxSession session, int size, boolean binary) {
        super(session, size, binary);
    }


    @Override
    public PromiseInternal<Void> rollback() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        PromiseInternal<Void> newPromise = VertxUtil.newPromise();
        transactionSession.rollback()
                .eventually(unused -> transactionSession.closeStatementState())
                .onComplete(event -> {
                    if (event.succeeded()) {
                        newPromise.tryComplete();
                        session.writeOk(count < size);
                    } else {
                        newPromise.fail(event.cause());
                        sendError(event.cause());
                    }
                });
        return newPromise;
    }

    @Override
    public PromiseInternal<Void> begin() {
        count++;
        PromiseInternal<Void> newPromise = VertxUtil.newPromise();
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.begin()
                .eventually(unused -> transactionSession.closeStatementState())
                .onComplete(event -> {
                    if (event.succeeded()) {
                        newPromise.tryComplete();
                        session.writeOk(count < size);
                    } else {
                        newPromise.fail(event.cause());
                        sendError(event.cause());
                    }
                });
        return newPromise;
    }

    @Override
    public PromiseInternal<Void> commit() {
        count++;
        PromiseInternal<Void> newPromise = VertxUtil.newPromise();
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.commit().eventually(unused -> transactionSession.closeStatementState())
                .onComplete(event -> {
                    if (event.succeeded()) {
                        newPromise.tryComplete();
                        session.writeOk(count < size);
                    } else {
                        newPromise.fail(event.cause());
                        sendError(event.cause());
                    }
                });
        return newPromise;
    }


}
