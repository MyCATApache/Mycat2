package io.mycat.runtime;

import io.mycat.MycatConnection;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.util.Dumper;

public class ProxyTransactionSession implements TransactionSession {
    private TransactionSession parent;

    public ProxyTransactionSession(TransactionSession parent) {
        this.parent = parent;
    }

    @Override
    public String name() {
        return "proxy";
    }

    @Override
    public void setTransactionIsolation(int transactionIsolation) {
        parent.setTransactionIsolation(transactionIsolation);
    }

    @Override
    public void begin() {
        parent.begin();
    }

    @Override
    public void commit() {
        parent.commit();
    }

    @Override
    public void rollback() {
        parent.rollback();
    }

    @Override
    public boolean isInTransaction() {
        return parent.isInTransaction();
    }

    @Override
    public void setAutocommit(boolean autocommit) {
        parent.setAutocommit(autocommit);
    }

    @Override
    public boolean isAutocommit() {
        return parent.isAutocommit();
    }

    @Override
    public MycatConnection getConnection(String targetName) {
        return parent.getConnection(targetName);
    }

    @Override
    public int getServerStatus() {
        return parent.getServerStatus();
    }

    @Override
    public boolean isReadOnly() {
        return parent.isReadOnly();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        parent.setReadOnly(readOnly);
    }

    @Override
    public int getTransactionIsolation() {
        return parent.getTransactionIsolation();
    }

    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.MULTI_THREADING;
    }

    @Override
    public void clearJdbcConnection() {
        parent.clearJdbcConnection();
    }

    @Override
    public void close() {
        parent.close();
    }

    @Override
    public String resolveFinalTargetName(String targetName) {
        return parent.resolveFinalTargetName(targetName);
    }

    @Override
    public TransactionType transactionType() {
        return TransactionType.PROXY_TRANSACTION_TYPE;
    }

    @Override
    public void ensureTranscation() {
        parent.ensureTranscation();
    }

    @Override
    public void addCloseResource(AutoCloseable closeable) {
        parent.addCloseResource(closeable);
    }

    @Override
    public String getTxId() {
        return parent.getTxId();
    }

    @Override
    public Dumper snapshot() {
        return parent.snapshot().addText("proxy");
    }
}