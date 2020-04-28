package io.mycat;

import io.mycat.beans.mycat.TransactionType;
import io.mycat.bindThread.BindThreadKey;

public interface SessionOpt extends Identical, BindThreadKey {

    TransactionType transactionType();

    TransactionSession getTransactionSession();

    void setTransactionSession(TransactionSession session);
}