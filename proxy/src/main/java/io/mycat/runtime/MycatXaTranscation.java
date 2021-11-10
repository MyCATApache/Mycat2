package io.mycat.runtime;

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.DataSourceNearness;
import io.mycat.ReplicaBalanceType;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.replica.DataSourceNearnessImpl;
import io.mycat.util.Dumper;
import io.vertx.core.Future;

import java.util.List;
import java.util.function.Function;

public class MycatXaTranscation implements XaSqlConnection, TransactionSession {
    protected final XaSqlConnection connection;
    protected final TransactionType transactionType;
    protected final DataSourceNearness dataSourceNearness = new DataSourceNearnessImpl(this);

    public MycatXaTranscation(XaSqlConnection connection, TransactionType transactionType) {
        this.connection = connection;
        this.transactionType = transactionType;
    }

    @Override
    public Dumper snapshot() {
        return Dumper.create();
    }

    @Override
    public String name() {
        return transactionType.name();
    }

    @Override
    public String resolveFinalTargetName(String targetName, boolean master, ReplicaBalanceType replicaBalanceType) {
        return dataSourceNearness.getDataSourceByTargetName(targetName, master, replicaBalanceType);

    }

    @Override
    public TransactionType transactionType() {
        return transactionType;
    }

    @Override
    public void setTransactionIsolation(MySQLIsolation level) {
        connection.setTransactionIsolation(level);
    }

    @Override
    public MySQLIsolation getTransactionIsolation() {
        return connection.getTransactionIsolation();
    }

    @Override
    public Future<Void> begin() {
        return connection.begin();
    }

    @Override
    public Future<NewMycatConnection> getConnection(String targetName) {
        return connection.getConnection(targetName);
    }

    @Override
    public List<NewMycatConnection> getExistedTranscationConnections() {
        return connection.getExistedTranscationConnections();
    }

    @Override
    public Future<Void> rollback() {
        return connection.rollback();
    }

    @Override
    public Future<Void> commit() {
        return connection.commit();
    }

    @Override
    public Future<Void> commitXa(Function<ImmutableCoordinatorLog, Future<Void>> beforeCommit) {
        return connection.commitXa(beforeCommit);
    }

    @Override
    public Future<Void> close() {
        return connection.close();
    }

    @Override
    public Future<Void> kill() {
        return connection.kill();
    }

    @Override
    public Future<Void> openStatementState() {
        return connection.openStatementState();
    }

    @Override
    public Future<Void> closeStatementState() {
        dataSourceNearness.clear();
        return connection.closeStatementState();
    }

    @Override
    public void setAutocommit(boolean b) {
        connection.setAutocommit(b);
    }

    @Override
    public boolean isAutocommit() {
        return connection.isAutocommit();
    }

    @Override
    public boolean isInTransaction() {
        return connection.isInTransaction();
    }

    @Override
    public String getXid() {
        return connection.getXid();
    }

    @Override
    public void addCloseFuture(Future<Void> future) {
        connection.addCloseFuture(future);
    }

    @Override
    public Future<Void> createSavepoint(String name) {
        return connection.createSavepoint(name);
    }

    @Override
    public Future<Void> rollbackSavepoint(String name) {
        return connection.rollbackSavepoint(name);
    }

    @Override
    public Future<Void> releaseSavepoint(String name) {
        return connection.releaseSavepoint(name);
    }

    @Override
    public List<NewMycatConnection> getAllConnections() {
        return connection.getAllConnections();
    }
}
