package io.mycat;

import io.mycat.beans.mycat.TransactionType;
import io.vertx.core.Vertx;

/**
 * @author Junwen Chen
 **/
public interface SessionOpt extends Identical {

    TransactionType transactionType();

    TransactionSession getTransactionSession();

    void setTransactionSession(TransactionSession session);

    public static void main(String[] args) {

    }
}