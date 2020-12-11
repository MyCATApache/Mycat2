package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.util.Dumper;

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
        return ThreadUsageEnum.MULTI_THREADING;
    }

    @Override
    public TransactionType transactionType() {
        return TransactionType.PROXY_TRANSACTION_TYPE;
    }

    @Override
    protected void callBackBegin() {
        super.callBackBegin();
    }

    @Override
    protected void callBackCommit() {
        super.callBackCommit();
    }

    @Override
    protected void callBackRollback() {
        super.callBackRollback();
    }
    @Override
    public Dumper snapshot() {
        return super.snapshot()
                .addText("name",name())
                .addText("threadUsage",getThreadUsageEnum())
                .addText("transactionType",this.transactionType());
    }
}