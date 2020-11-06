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

import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import lombok.SneakyThrows;

import javax.transaction.UserTransaction;

/**
 * @author Junwen Chen
 **/
public class JTATransactionSession extends TransactionSessionTemplate implements TransactionSession {

    private static final MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JTATransactionSession.class);
    private final UserTransaction userTransaction;


    public JTATransactionSession(UserTransaction userTransaction) {
        this.userTransaction = userTransaction;
    }

    @Override
    @SneakyThrows
    protected void callBackBegin() {
        userTransaction.begin();
    }

    @Override
    @SneakyThrows
    protected void callBackCommit() {
        userTransaction.commit();
    }

    @Override
    @SneakyThrows
    protected void callBackRollback() {
        userTransaction.rollback();
    }

    @Override
    @SneakyThrows
    protected DefaultConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly) {
        return updateConnectionMap.compute(jdbcDataSource,
                (dataSource, absractConnection) -> {
                    if (absractConnection != null && !absractConnection.isClosed()) {
                        return absractConnection;
                    } else {
                        return JdbcRuntime.INSTANCE
                                .getConnection(jdbcDataSource, autocommit, transactionIsolation,readOnly);
                    }
                });
    }



    @Override
    public void bind(String key, String type) {

    }


    @Override
    public boolean needBindThread() {
        return isInTransaction();
    }

}