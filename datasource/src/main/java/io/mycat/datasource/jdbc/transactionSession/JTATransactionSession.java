/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc.transactionSession;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.XATranscationStatusUtil;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.util.Dumper;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author Junwen Chen
 **/
public class JTATransactionSession extends TransactionSessionTemplate implements TransactionSession {

    private static final Logger LOGGER = LoggerFactory.getLogger(JTATransactionSession.class);
    private final Supplier<UserTransaction> userTransactionProvider;
    private UserTransaction userTransaction;
    private Thread bindThread;


    public JTATransactionSession(MycatDataContext dataContext, Supplier<UserTransaction> userTransactionProvider) {
        super(dataContext);
        this.userTransactionProvider = userTransactionProvider;
    }

    @Override
    @SneakyThrows
    protected void callBackBegin() {
        this.bindThread = Thread.currentThread();
        userTransaction = userTransactionProvider.get();
        userTransaction.begin();
    }

    @Override
    @SneakyThrows
    protected void callBackCommit() {
        if (bindThread != Thread.currentThread()) {
            throw new AssertionError();
        }
        this.userTransaction.commit();
        this.userTransaction = null;
        this.bindThread = null;
    }

    @Override
    @SneakyThrows
    protected void callBackRollback() {
        if (bindThread != Thread.currentThread()) {
            throw new AssertionError();
        }
        this.userTransaction.rollback();
        this.userTransaction = null;
        this.bindThread = null;
    }

    @Override
    @SneakyThrows
    protected DefaultConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly) {
        return updateConnectionMap.compute(jdbcDataSource,
                (dataSource, absractConnection) -> {
                    if (absractConnection != null && !absractConnection.isClosed()) {
                        return absractConnection;
                    } else {
                        return JdbcRuntime.INSTANCE//jta不使用连接本身的autocommit开启事务
                                .getConnection(jdbcDataSource, null, transactionIsolation, readOnly);
                    }
                });
    }


    @Override
    public String name() {
        return "xa";
    }

    @Override
    @SneakyThrows
    public void close() {
        if (isInTransaction() && userTransaction != null) {
            try {
                this.userTransaction.rollback();
            } catch (Throwable e) {
                LOGGER.error("", e);
                this.userTransaction.setRollbackOnly();
            }
            this.userTransaction = null;
            this.bindThread = null;
        }
        super.close();
    }

    @Override
    public TransactionType transactionType() {
        return TransactionType.JDBC_TRANSACTION_TYPE;
    }


    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.BINDING_THREADING;
    }

    /////////////////////////////////////////debug////////////////////////////////////////////////
    @Override
    public synchronized Dumper snapshot() {
        Dumper top = super.snapshot();
        String useTransaction = Optional.ofNullable(this.userTransaction)
                .map(i -> {
                    try {
                        return XATranscationStatusUtil.toText(i.getStatus());
                    } catch (SystemException e) {
                        return e.getMessage();
                    }
                }).orElse("");
        Optional<Thread> bindThread = Optional.ofNullable(this.bindThread);
        String name = bindThread.map(i -> i.getName()).orElse("");
        Long id = bindThread.map(i -> i.getId()).orElse(null);
        return top.addText("threadName", name).addText("threadId", id).addText("useTransactionStatus", useTransaction);
    }

}