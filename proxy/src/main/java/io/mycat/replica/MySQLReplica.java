/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica;

import io.mycat.beans.mycat.MycatReplica;
import io.mycat.beans.mysql.MySQLCollationIndex;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.plug.loadBalance.BalanceAllRead;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.MycatExpection;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class MySQLReplica implements MycatReplica {
    private final ReplicaConfig config;
    private volatile int writeIndex = 0; //主节点默认为0
    private long lastInitTime;  //最后一次初始化时间
    private List<MySQLDatasource> datasourceList = new ArrayList<>();
    private LoadBalanceStrategy defaultLoadBalanceStrategy = BalanceAllRead.INSTANCE;
    private MySQLCollationIndex collationIndex;

    public MySQLReplica(ReplicaConfig replicaConfig, int writeIndex) {
        List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
        checkIndex(writeIndex, mysqls.size());
        this.config = replicaConfig;
        for (int index = 0; index < mysqls.size(); index++) {
            boolean master = index == writeIndex;
            DatasourceConfig datasourceConfig = mysqls.get(index);
            if (datasourceConfig.getDbType() == null){
                datasourceList.add(new MySQLDatasource(index, master,datasourceConfig , this));
            }
        }
    }

    public long getLastInitTime() {
        return lastInitTime;
    }

    public void init() {
        BiConsumer<MySQLDatasource, Boolean> defaultCallBack = (datasource, success) -> {
            this.lastInitTime = System.currentTimeMillis();
            this.collationIndex =datasource.getCollationIndex();
        };
        for (MySQLDatasource datasource : datasourceList) {
            datasource.init(defaultCallBack);
        }
    }

    public MySQLCollationIndex getCollationIndex() {
        return collationIndex;
    }


    public void getMySQLSessionByBalance(boolean runOnSlave, LoadBalanceStrategy strategy, AsynTaskCallBack<MySQLClientSession> asynTaskCallBack) {
        MySQLDatasource datasource;
        if (!runOnSlave) {
            getWriteDatasource(asynTaskCallBack);
            return;
        }
        if (strategy == null) {
            strategy = this.defaultLoadBalanceStrategy;
        }
        datasource = strategy.select(this, writeIndex, this.datasourceList);
        if (datasource == null) {
            getWriteDatasource(asynTaskCallBack);
        } else {
            getDatasource(asynTaskCallBack, datasource);
        }
    }

    private void getWriteDatasource(AsynTaskCallBack<MySQLClientSession> asynTaskCallBack) {
        MySQLDatasource datasource = this.datasourceList.get(writeIndex);
        getDatasource(asynTaskCallBack, datasource);
        return;
    }

    private void getDatasource(AsynTaskCallBack<MySQLClientSession> asynTaskCallBack, MySQLDatasource datasource) {
        if (Thread.currentThread() instanceof MycatReactorThread){
            MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
            reactor.getMySQLSessionManager().getIdleSessionsOfKey(datasource, asynTaskCallBack);
        }else {
          throw new MycatExpection("unsupport!");
        }
    }

    public void doHeartbeat() {
        MySQLDatasource master = datasourceList.get(writeIndex);
        if (master == null) return;
        for (MySQLDatasource ds : datasourceList) {
            if (ds != null) {
                ds.doHeartbeat();
            }
        }
    }

    private void checkIndex(int newIndex, int size) {
        if (newIndex < 0 || newIndex >= size) {
            throw new MycatExpection("index out of dataSouce size");
        }
    }

    public String getName() {
        return config.getName();
    }

}
