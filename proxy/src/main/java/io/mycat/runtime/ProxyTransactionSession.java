package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.util.Dumper;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

public class ProxyTransactionSession extends LocalTransactionSession {
    public ProxyTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String name() {
        return "proxy";
    }

    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.THIS_THREADING;
    }

    @Override
    public TransactionType transactionType() {
        return TransactionType.PROXY_TRANSACTION_TYPE;
    }

    @Override
    protected void callBackBegin() {

    }

    @Override
    protected void callBackCommit() {

    }

    @Override
    protected void callBackRollback() {

    }
    @Override
    public Dumper snapshot() {
        return super.snapshot()
                .addText("name",name())
                .addText("threadUsage",getThreadUsageEnum())
                .addText("transactionType",this.transactionType());
    }
}