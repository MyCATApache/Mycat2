package io.mycat.runtime;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.XaSqlConnection;
import cn.mycat.vertx.xa.impl.BaseXaSqlConnection;
import io.mycat.MycatConnection;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.util.Dumper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.function.Supplier;

public class ProxyTransactionSession extends BaseXaSqlConnection implements TransactionSession{
    private TransactionSession parent;
    private XaSqlConnection connection;

    public ProxyTransactionSession(Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog, TransactionSession parent) {
        super(mySQLManagerSupplier,xaLog);
        this.parent = parent;
        this.connection = new BaseXaSqlConnection(mySQLManagerSupplier,xaLog);
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
    public Future<Void> begin() {
        return (Future)CompositeFuture.all(parent.begin(),connection.begin());
    }

    @Override
    public Future<Void> commit() {
        return (Future)CompositeFuture.all(parent.commit(),connection.commit());
    }

    @Override
    public Future<Void> rollback() {
        return (Future)CompositeFuture.all(parent.rollback(),connection.rollback());
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
    public MycatConnection getJDBCConnection(String targetName) {
        return parent.getJDBCConnection(targetName);
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
    public Future<Void> closeStatenmentState() {
        parent.closeStatenmentState();
        connection.closeStatementState();
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> close() {
        return parent.close();
    }

    @Override
    public String resolveFinalTargetName(String targetName) {
        return parent.resolveFinalTargetName(targetName);
    }

    @Override
    public String resolveFinalTargetName(String targetName, boolean master) {
        return parent.resolveFinalTargetName(targetName, master);
    }

    @Override
    public TransactionType transactionType() {
        return TransactionType.PROXY_TRANSACTION_TYPE;
    }

    @Override
    public Future<Void> openStatementState() {
        parent.openStatementState();
        connection.openStatementState();
        return Future.succeededFuture();
    }

    @Override
    public void addCloseResource(AutoCloseable closeable) {
        parent.addCloseResource(closeable);
    }

    @Override
    public String getTxId() {
        return connection.getXid();
    }

    @Override
    public Dumper snapshot() {
        return parent.snapshot().addText("proxy");
    }
}