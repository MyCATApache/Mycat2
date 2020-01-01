package io.mycat.client;

import java.util.List;

public interface MycatClient {
    public Context analysis(String sql) ;
    public List<String> explain(String sql);
    public void useSchema(String schemaName);
    public String getTransactionType();
    public void useTransactionType(String transactionType);
}