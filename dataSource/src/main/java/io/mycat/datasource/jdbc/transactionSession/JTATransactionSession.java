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
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import lombok.SneakyThrows;

import javax.transaction.UserTransaction;
import java.util.function.Supplier;

/**
 * @author Junwen Chen
 **/
public class JTATransactionSession extends TransactionSessionTemplate implements TransactionSession {

    private static final MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JTATransactionSession.class);
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
            this.userTransaction.setRollbackOnly();
            this.userTransaction = null;
            this.bindThread = null;
        }
        super.close();
    }

    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.BINDING_THREADING;
    }

}