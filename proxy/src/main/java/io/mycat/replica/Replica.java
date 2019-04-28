package io.mycat.replica;

import io.mycat.beans.mysql.MySQLCollationIndex;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.plug.loadBalance.BalanceAllRead;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.AsynTaskCallBack;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

public class Replica {
    private final ReplicaConfig config;
    private volatile int writeIndex = 0; //主节点默认为0
    private long lastInitTime;  //最后一次初始化时间
    private List<Datasource> datasourceList = new ArrayList<>();
    private LoadBalanceStrategy defaultLoadBalanceStrategy = BalanceAllRead.INSTANCE;
    private MySQLCollationIndex collationIndex;

    public Replica(ReplicaConfig replicaConfig, int writeIndex) {
        List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
        checkIndex(writeIndex, mysqls.size());
        this.config = replicaConfig;
        for (int index = 0; index < mysqls.size(); index++) {
            boolean master = index == writeIndex;
            datasourceList.add(new Datasource(index, master, mysqls.get(index), this));
        }
    }

    public long getLastInitTime() {
        return lastInitTime;
    }

    public void init() {
        BiConsumer<Datasource, Boolean> defaultCallBack = (datasource, success) -> {
            this.lastInitTime = System.currentTimeMillis();
            this.collationIndex =datasource.getCollationIndex();
        };
        for (Datasource datasource : datasourceList) {
            datasource.init(defaultCallBack);
        }
    }

    public MySQLCollationIndex getCollationIndex() {
        return collationIndex;
    }


    public void getMySQLSessionByBalance(boolean runOnSlave, LoadBalanceStrategy strategy, AsynTaskCallBack<MySQLSession> asynTaskCallBack) {
        Datasource datasource;
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

    private void getWriteDatasource(AsynTaskCallBack<MySQLSession> asynTaskCallBack) {
        Datasource datasource = this.datasourceList.get(writeIndex);
        getDatasource(asynTaskCallBack, datasource);
        return;
    }

    private void getDatasource(AsynTaskCallBack<MySQLSession> asynTaskCallBack, Datasource datasource) {
        if (Thread.currentThread() instanceof MycatReactorThread){
            MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
            reactor.getMySQLSessionManager().getIdleSessionsOfKey(datasource, asynTaskCallBack);
        }else {
          throw new MycatExpection("unsupport!");
        }
    }

    public void doHeartbeat() {
        Datasource master = datasourceList.get(writeIndex);
        if (master == null) return;
        for (Datasource ds : datasourceList) {
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
