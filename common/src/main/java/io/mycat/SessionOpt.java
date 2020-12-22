package io.mycat;

import io.mycat.beans.mycat.TransactionType;
import io.mycat.bindthread.BindThreadKey;

/**
 * @author Junwen Chen
 **/
public interface SessionOpt extends Identical, BindThreadKey {

    TransactionType transactionType();

    TransactionSession getTransactionSession();

    void setTransactionSession(TransactionSession session);
}