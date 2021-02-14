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

import java.util.function.Function;
import java.util.function.Supplier;

public class ProxyTransactionSession extends BaseXaSqlConnection implements TransactionSession{
    private TransactionSession parent;

    public ProxyTransactionSession(Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog, TransactionSession parent) {
        super(mySQLManagerSupplier,xaLog);
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
    public void addCloseResource(AutoCloseable closeable) {
        parent.addCloseResource(closeable);
    }

    @Override
    public Dumper snapshot() {
        return parent.snapshot().addText("proxy");
    }
}