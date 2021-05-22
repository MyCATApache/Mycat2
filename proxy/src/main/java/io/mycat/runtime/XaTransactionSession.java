/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.runtime;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.impl.LocalSqlConnection;
import cn.mycat.vertx.xa.impl.LocalXaSqlConnection;
import io.mycat.DataSourceNearness;
import io.mycat.ReplicaBalanceType;
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

    @Override
    public String resolveFinalTargetName(String targetName, boolean master, ReplicaBalanceType replicaBalanceType) {
        return dataSourceNearness.getDataSourceByTargetName(targetName,master,replicaBalanceType);
    }
}