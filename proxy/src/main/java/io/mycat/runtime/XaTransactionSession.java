package io.mycat.runtime;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.impl.LocalSqlConnection;
import cn.mycat.vertx.xa.impl.LocalXaSqlConnection;
import io.mycat.DataSourceNearness;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.replica.DataSourceNearnessImpl;
import io.mycat.util.Dumper;
import io.vertx.core.Future;

import java.util.function.Supplier;

public class XaTransactionSession extends LocalXaSqlConnection implements TransactionSession {
    protected final DataSourceNearness dataSourceNearness = new DataSourceNearnessImpl(this);
    public XaTransactionSession(Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog) {
        super(mySQLManagerSupplier, xaLog);
    }

    @Override
    public String name() {
        return "xa";
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
    public TransactionType transactionType() {
        return TransactionType.JDBC_TRANSACTION_TYPE;
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