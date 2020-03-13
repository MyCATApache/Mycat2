package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.transactionSession.TransactionSessionTemplate;

public class ProxyTransactionSession extends TransactionSessionTemplate implements TransactionSession {
    public ProxyTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String name() {
        return "proxy";
    }

    @Override
    public boolean needBindThread() {
        return false;
    }

    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.THIS_THREADING;
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
    protected DefaultConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly) {
        throw new UnsupportedOperationException();
    }
}