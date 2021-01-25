package io.mycat.datasource.jdbc.transactionsession;

import io.mycat.*;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.Dumper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class LocalTransactionSession extends TransactionSessionTemplate implements TransactionSession {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LocalTransactionSession.class);

    public LocalTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    private String nextXid() {
        return null;
    }

    @Override
    public String name() {
        return "xa";
    }

    @Override
    @SneakyThrows
    public MycatConnection getJDBCConnection(String targetName) {
            targetName = resolveFinalTargetName(targetName);
            DefaultConnection defaultConnection = updateConnectionMap.get(targetName);
            if (defaultConnection != null) {
                if (defaultConnection.getRawConnection().getAutoCommit()) {
                    if (isInTransaction()) {
                        defaultConnection.getRawConnection().setAutoCommit(false);
                    }
                }
                return (defaultConnection);
            }
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            DefaultConnection connection = jdbcConnectionManager.getConnection(targetName, isAutocommit(), getTransactionIsolation(), false);
            updateConnectionMap.put(targetName, connection);
            return (connection);
    }


    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.MULTI_THREADING;
    }

    @Override
    public TransactionType transactionType() {
        return TransactionType.JDBC_TRANSACTION_TYPE;
    }

    @Override
    protected void callBackBegin() {
        for (DefaultConnection value : this.updateConnectionMap.values()) {
            value.close();
        }
        this.updateConnectionMap.clear();
    }

    @Override
    protected void callBackCommit() {
        ArrayList<SQLException> exceptions = new ArrayList<>();
        for (DefaultConnection value : this.updateConnectionMap.values()) {
            try {
                value.getRawConnection().commit();
            } catch (SQLException e) {
                LOGGER.error("本地事务提交失败", e);
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty()) {
            throw new MycatException("本地事务提交失败\n" + exceptions.stream().map(i -> i.getMessage()).collect(Collectors.joining("\n")));
        }
    }

    @Override
    protected void callBackRollback() {
        ArrayList<SQLException> exceptions = new ArrayList<>();
        for (DefaultConnection value : this.updateConnectionMap.values()) {
            try {
                value.getRawConnection().rollback();
            } catch (SQLException e) {
                LOGGER.error("本地事务回滚失败", e);
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty()) {
            throw new MycatException("本地事务回滚失败\n" + exceptions.stream().map(i -> i.getMessage()).collect(Collectors.joining("\n")));
        }
    }

    @Override
    public Dumper snapshot() {
        return super.snapshot()
                .addText("name", name())
                .addText("threadUsage", getThreadUsageEnum())
                .addText("transactionType", this.transactionType());
    }

    @Override
    public String getTxId() {
        return null;
    }
}