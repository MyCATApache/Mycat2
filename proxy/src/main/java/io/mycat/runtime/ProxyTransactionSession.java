package io.mycat.runtime;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.impl.BaseXaSqlConnection;
import cn.mycat.vertx.xa.impl.LocalSqlConnection;
import cn.mycat.vertx.xa.impl.LocalXaSqlConnection;
import io.mycat.DataSourceNearness;
import io.mycat.MycatConnection;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.replica.DataSourceNearnessImpl;
import io.mycat.util.Dumper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.function.Supplier;

public class ProxyTransactionSession extends LocalSqlConnection implements TransactionSession {
    protected final DataSourceNearness dataSourceNearness = new DataSourceNearnessImpl(this);
    public ProxyTransactionSession(Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog) {
        super(mySQLManagerSupplier, xaLog);
    }

    @Override
    public String name() {
        return "proxy";
    }

    @Override
    public String resolveFinalTargetName(String targetName) {
        return dataSourceNearness.getDataSourceByTargetName(targetName);
    }

    @Override
    public String resolveFinalTargetName(String targetName, boolean master) {
        return dataSourceNearness.getDataSourceByTargetName(targetName, master);
    }

    @Override
    public Dumper snapshot() {
        return Dumper.create();
    }

    @Override
    public Future<Void> closeStatementState() {
        dataSourceNearness.clear();
        return super.closeStatementState();
    }
}