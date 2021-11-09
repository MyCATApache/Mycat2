package cn.mycat.vertx.xa;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSavePointStatement;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.newquery.NewMycatConnection;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SavepointSqlConnection implements XaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(SavepointSqlConnection.class);
    final XaSqlConnection connection;
    protected final List<String> savepoints = new ArrayList<>();

    public SavepointSqlConnection(XaSqlConnection connection) {
        this.connection = connection;
    }

    public Future<Void> createSavepoint(String name) {
        savepoints.add(name);
        String sqls = buildSavepointSql(name);
        Future<Void> execute = execute(sqls);
        return check(execute);
    }

    public Future<Void> rollbackSavepoint(String name) {
        savepoints.remove(name);
        Future<Void> execute = execute(buildRollbackSavepointSql(name));
        return check(execute);
    }

    public Future<Void> releaseSavepoint(String name) {
        savepoints.remove(name);
        Future<Void> execute = execute(buildReleaseSavepointSql(name));
        return check(execute);
    }

    private Future<Void> check(Future<Void> execute) {
        return execute.recover(throwable -> {
            LOGGER.error("", throwable);
            return close().flatMap(unused -> {
                return (Future)Future.failedFuture(throwable);
            });
        });
    }

    private Future<Void> execute(String sqls) {
        List<NewMycatConnection> existedTranscationConnections = this.connection.getExistedTranscationConnections();
        List<Future> futures = new ArrayList<>(existedTranscationConnections.size());
        for (NewMycatConnection existedTranscationConnection : existedTranscationConnections) {
            futures.add(existedTranscationConnection.update(sqls));
        }
        return CompositeFuture.join(futures).mapEmpty();
    }


    private String buildSavepointSql(String name) {
        SQLSavePointStatement statement = new SQLSavePointStatement(DbType.mysql);
        statement.setName(new SQLIdentifierExpr(name));
        return statement.toString();
    }

    private String buildRollbackSavepointSql(String name) {
        SQLRollbackStatement sqlRollbackStatement = new SQLRollbackStatement();
        sqlRollbackStatement.setTo(new SQLIdentifierExpr(name));
        return sqlRollbackStatement.toString();
    }

    private String buildReleaseSavepointSql(String name) {
        SQLReleaseSavePointStatement statement = new SQLReleaseSavePointStatement(DbType.mysql);
        statement.setName(new SQLIdentifierExpr(name));
        return statement.toString();
    }
    @Override
    public List<NewMycatConnection> getExistedTranscationConnections() {
        return connection.getExistedTranscationConnections();
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
        Future<NewMycatConnection> connectionFuture = this.connection.getConnection(targetName);
        if (savepoints.isEmpty()) {
            return connectionFuture;
        } else {
            String sqls = savepoints.stream().map(name -> buildSavepointSql(name)).collect(Collectors.joining(";"));
            return connectionFuture.flatMap(connection -> connection.update(sqls).mapEmpty());
        }

    }

    @Override
    public Future<Void> rollback() {
        return connection.rollback().onComplete(voidAsyncResult -> savepoints.clear());
    }

    @Override
    public Future<Void> commit() {
        return connection.commit().onComplete(voidAsyncResult -> savepoints.clear());
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
    public Future<Void> openStatementState() {
        return connection.openStatementState();
    }

    @Override
    public Future<Void> closeStatementState() {
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
}
