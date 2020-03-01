package io.mycat.datasource.jdbc.transactionSession;

import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.TransactionSession;

public class LocalTransactionSession extends TransactionSessionTemplate implements TransactionSession {
    @Override
    public void bind(String key, String type) {

    }

    @Override
    public boolean needBindThread() {
        return false;
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