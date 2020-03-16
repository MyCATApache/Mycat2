package io.mycat.datasource.jdbc.transactionSession;

import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.TransactionSession;

import java.util.Map;

public class MultipleTransactionSession implements TransactionSession {
    final Map<String, TransactionSession> map;
    volatile TransactionSession currentSession;

    public MultipleTransactionSession(Map<String, TransactionSession> map) {
        this.map = map;
    }

    @Override
    public void setTransactionIsolation(int transactionIsolation) {
        currentSession.setTransactionIsolation(transactionIsolation);
    }

    @Override
    public void bind(String key, String type) {
        currentSession = map.get(type);
        currentSession.bind(key, type);
    }

    @Override
    public void begin() {
        currentSession.begin();
    }

    @Override
    public void commit() {
        currentSession.commit();
    }

    @Override
    public void rollback() {
        currentSession.rollback();
    }

    @Override
    public boolean isInTransaction() {
        return currentSession.isInTransaction();
    }

    @Override
    public void setAutocommit(boolean autocommit) {
        currentSession.setAutocommit(autocommit);

    }

    @Override
    public boolean isAutocommit() {
        return currentSession.isAutocommit();
    }

    @Override
    public DefaultConnection getConnection(String jdbcDataSource) {
        return currentSession.getConnection(jdbcDataSource);
    }

    @Override
    public void reset() {
        currentSession.reset();
    }

    @Override
    public int getServerStatus() {
        return currentSession.getServerStatus();
    }

    @Override
    public void onEndOfResponse() {
        currentSession.onEndOfResponse();
    }

    @Override
    public boolean isReadOnly() {
        return currentSession.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        currentSession.setReadOnly(readOnly);
    }

    @Override
    public boolean needBindThread() {
        return currentSession.needBindThread();
    }

    @Override
    public int getTransactionIsolation() {
        return currentSession.getTransactionIsolation();
    }
}