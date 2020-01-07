package io.mycat.client;

import io.mycat.beans.mycat.TransactionType;

import java.util.List;

public interface MycatClient {
    public Context analysis(String sql) ;
    public List<String> explain(String sql);
    public void useSchema(String schemaName);
    public TransactionType getTransactionType();
    public void useTransactionType(TransactionType transactionType);

    String getDefaultSchema();

}