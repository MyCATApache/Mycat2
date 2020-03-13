package io.mycat;

import io.mycat.bindThread.BindThreadKey;

public interface SessionOpt extends Identical, BindThreadKey {

    String transactionType();

    TransactionSession getTransactionSession();

    void setTransactionSession(TransactionSession session);
}