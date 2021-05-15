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
package io.mycat.replica;

import io.mycat.DataSourceNearness;
import io.mycat.MetaClusterCurrent;
import io.mycat.ReplicaBalanceType;
import io.mycat.TransactionSession;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使集群选择数据源具有亲近性
 *
 * @junwen12221
 */
public class DataSourceNearnessImpl implements DataSourceNearness {
    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    String loadBalanceStrategy;

    private TransactionSession transactionSession;

    public DataSourceNearnessImpl(TransactionSession transactionSession) {
        this.transactionSession = transactionSession;
    }

    public synchronized String getDataSourceByTargetName(final String targetName, boolean masterArg, ReplicaBalanceType replicaBalanceType) {
        Objects.requireNonNull(targetName);
        boolean master = masterArg || transactionSession.isInTransaction();
        ReplicaSelectorManager selector = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        boolean replicaMode = selector.isReplicaName(targetName);
        String datasource;
        if (replicaMode) {
            datasource = map.computeIfAbsent(targetName, (s) -> {
                String datasourceNameByReplicaName = selector.getDatasourceNameByReplicaName(targetName, master,replicaBalanceType, loadBalanceStrategy);
                return Objects.requireNonNull(datasourceNameByReplicaName);
            });
        } else {
            datasource = targetName;
        }
        return Objects.requireNonNull(datasource);
    }

    @Override
    public String getDataSourceByTargetName(String targetName) {
        return getDataSourceByTargetName(targetName, false,ReplicaBalanceType.NONE);
    }

    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }


    public void clear() {
        map.clear();
        loadBalanceStrategy = null;
    }
}