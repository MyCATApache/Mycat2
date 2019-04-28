package io.mycat.beans;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.CommandTask;
import io.mycat.proxy.task.MultiOkQueriesCounterTask;
import io.mycat.replica.Replica;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class DataNode {
    private String dataNodeName;
    private String databaseName;
    private Replica replica;

    public DataNode(String dataNodeName, String databaseName, Replica replica) {
        this.dataNodeName = dataNodeName;
        this.databaseName = databaseName;
        this.replica = replica;
    }

    public String getDataNodeName() {
        return dataNodeName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Replica getReplica() {
        return replica;
    }

    public void getMySQLSessionFromUserThread(MySQLIsolation isolation, MySQLAutoCommit autoCommit, String charSet,
                                              boolean runOnSlave, LoadBalanceStrategy strategy, AsynTaskCallBack<MySQLSession> asynTaskCallBack){
        MycatReactorThread[] threads = MycatRuntime.INSTANCE.getMycatReactorThreads();
        int i = ThreadLocalRandom.current().nextInt(0, threads.length);
        threads[i].addNIOJob(()->{ getMySQLSession(isolation, autoCommit, charSet, runOnSlave, strategy, asynTaskCallBack); });
    }
    public void getMySQLSession(MySQLIsolation isolation, MySQLAutoCommit autoCommit, String charSet,
                                boolean runOnSlave, LoadBalanceStrategy strategy, AsynTaskCallBack<MySQLSession> asynTaskCallBack) {
        this.getReplica().getMySQLSessionByBalance(runOnSlave, strategy, (mysql, sender, success, result, errorMessage) -> {
            if (success) {
                if (this.equals(mysql.getDataNode())) {
                    asynTaskCallBack.finished(mysql, sender, true, result, errorMessage);
                } else {
                    new MultiOkQueriesCounterTask(3)
                            .request(mysql,isolation.getCmd() + autoCommit.getCmd() + "SET names " + charSet + ";", (session, sender1, success1, result1, errorMessage1) -> {
                                if (success1) {
                                    new CommandTask().request(session, 2, this.getDatabaseName(),asynTaskCallBack);
                                } else {
                                    asynTaskCallBack.finished(mysql, this, false, null, errorMessage1);
                                }
                            });
                }
            } else {
                asynTaskCallBack.finished(mysql, sender, success, result, errorMessage);
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataNode dataNode = (DataNode) o;
        return Objects.equals(dataNodeName, dataNode.dataNodeName) &&
                Objects.equals(databaseName, dataNode.databaseName) &&
                Objects.equals(replica, dataNode.replica);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataNodeName, databaseName, replica);
    }
}
