package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.transactionSession.TransactionSessionTemplate;

public class LocalTransactionSession extends TransactionSessionTemplate implements TransactionSession {
    public LocalTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String name() {
        return "local";
    }

    @Override
    public boolean needBindThread() {
        return false;
    }

    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.MULTI_THREADING;
    }

    @Override
    protected void callBackBegin() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void callBackCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void callBackRollback() {

    }

    @Override
    protected DefaultConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly) {
        return updateConnectionMap.compute(jdbcDataSource,
                (dataSource, absractConnection) -> {
                    if (absractConnection != null && !absractConnection.isClosed()) {
                        return absractConnection;
                    } else {
                        return JdbcRuntime.INSTANCE
                                .getConnection(jdbcDataSource, autocommit, transactionIsolation, readOnly);
                    }
                });
    }
}