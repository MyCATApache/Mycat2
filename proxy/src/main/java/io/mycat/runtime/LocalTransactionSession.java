package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.transactionsession.TransactionSessionTemplate;
import io.mycat.util.Dumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

public class LocalTransactionSession extends TransactionSessionTemplate implements TransactionSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTransactionSession.class);
    public LocalTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String name() {
        return "xa";
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
        ArrayList<SQLException> exceptions = new ArrayList<>();
        for (DefaultConnection i : this.updateConnectionMap.values()) {
            try {
                i.getRawConnection().setAutoCommit(false);
            } catch (SQLException e) {
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty()){
            throw new MycatException("本地事务开启失败\n"+exceptions.stream().map(i->i.getMessage()).collect(Collectors.joining("\n")));
        }
    }

    @Override
    protected void callBackCommit() {
        ArrayList<SQLException> exceptions = new ArrayList<>();
        for (DefaultConnection value : this.updateConnectionMap.values()) {
            try {
                value.getRawConnection().commit();
            } catch (SQLException e) {
                LOGGER.error("本地事务提交失败",e);
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty()){
            throw new MycatException("本地事务提交失败\n"+exceptions.stream().map(i->i.getMessage()).collect(Collectors.joining("\n")));
        }
    }

    @Override
    protected void callBackRollback() {
        ArrayList<SQLException> exceptions = new ArrayList<>();
        for (DefaultConnection value : this.updateConnectionMap.values()) {
            try {
                value.getRawConnection().rollback();
            } catch (SQLException e) {
                LOGGER.error("本地事务回滚失败",e);
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty()){
            throw new MycatException("本地事务回滚失败\n"+exceptions.stream().map(i->i.getMessage()).collect(Collectors.joining("\n")));
        }
    }

    @Override
    protected DefaultConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly) {
        Objects.requireNonNull(jdbcDataSource);
        return updateConnectionMap.compute(jdbcDataSource,
                (dataSource, absractConnection) -> {
                    if (absractConnection != null && !absractConnection.isClosed()) {
                        return absractConnection;
                    } else {
                        return JdbcRuntime.INSTANCE
                                .getConnection(jdbcDataSource, isAutocommit() && !isInTransaction(), TRANSACTION_REPEATABLE_READ, false);
                    }
                });
    }

    @Override
    public Dumper snapshot() {
        return super.snapshot()
                .addText("name",name())
                .addText("threadUsage",getThreadUsageEnum())
                .addText("transactionType",this.transactionType());
    }
}