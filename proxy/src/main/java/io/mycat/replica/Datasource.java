package io.mycat.replica;

import io.mycat.beans.mysql.MySQLCollationIndex;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.BackendCharsetReadTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

public class Datasource {
    private static final Logger logger = LoggerFactory.getLogger(Datasource.class);
    private final int index;
    private boolean master = false;
    private final DatasourceConfig datasourceConfig;
    private Replica replica;
    private MySQLCollationIndex collationIndex = new MySQLCollationIndex();



    public Datasource(int index, boolean master, DatasourceConfig datasourceConfig, Replica replica) {
        this.index = index;
        this.master = master;
        this.datasourceConfig = datasourceConfig;
        this.replica = replica;
    }

    public void init(BiConsumer<Datasource, Boolean> successCallback) {
        int minCon = datasourceConfig.getMinCon();
        MycatReactorThread[] threads = MycatRuntime.INSTANCE.getMycatReactorThreads();
        MycatReactorThread firstThread = threads[0 % threads.length];
        firstThread.addNIOJob(createMySQLSession(firstThread, (mysql0, sender0, success0, result0, errorMessage0) -> {
            if (success0) {
                logger.info("dataSource create successful!!");
                new BackendCharsetReadTask(this.collationIndex).request(mysql0,"SHOW COLLATION;", (mysql1, sender1, success1, result1, errorMessage1) -> {
                    if (success1) {
                        firstThread.getMySQLSessionManager().addIdleSession(mysql1);
                        logger.info("dataSource read charset successful!!");
                        for (int index = 1; index < minCon; index++) {
                            MycatReactorThread thread = threads[index % threads.length];
                            Integer finalIndex = index;
                            thread.addNIOJob(createMySQLSession(thread, (mysql2, sender2, success2, result2, errorMessage2) -> {
                                if (success2) {
                                    logger.info("dataSource {} create successful!!", finalIndex);
                                } else {
                                    logger.error("dataSource {} create fail!!", finalIndex);
                                }
                            }));
                        }
                        successCallback.accept(this, true);
                    } else {
                        logger.error("read charset fail", errorMessage1);
                        successCallback.accept(this, false);
                    }
                });
            } else {
                logger.error("dataSource {} create fail!!", 0);
                successCallback.accept(this, false);
            }
        }));
    }

    private Runnable createMySQLSession(MycatReactorThread thread, AsynTaskCallBack<MySQLSession> callback) {
       return ()-> thread.getMySQLSessionManager().createSession(this, (mysql, sender, success, result, errorMessage) -> {
            if (success) {
                callback.finished(mysql,this,true,null,null);
            } else {
                logger.error("create connection fail", errorMessage);
                callback.finished(null, this, false, null, errorMessage);
            }
        });
    }

    public void clearAndDestroyCons(String reason) {
        for (MycatReactorThread thread : MycatRuntime.INSTANCE.getMycatReactorThreads()) {
            thread.addNIOJob(() -> thread.getMySQLSessionManager().clearAndDestroyMySQLSession(this, reason));
        }
    }

    public String getName() {
        return this.datasourceConfig.getHostName();
    }
    public String getIp() {
        return this.datasourceConfig.getIp();
    }
    public int getPort() {
        return this.datasourceConfig.getPort();
    }
    public String getUsername() {
        return this.datasourceConfig.getUser();
    }
    public String getPassword() {
        return this.datasourceConfig.getPassword();
    }
    public boolean isAlive() {
        return true;
    }

    public boolean isActive() {
        return isAlive() && true;
    }

    public boolean canSelectAsReadNode() {
        return canSelectAsReadNode(HeartbeatInfReceiver.identity());
    }

    public boolean canSelectAsReadNode(HeartbeatInfReceiver receiver) {
        return true;
    }

    public boolean isMaster() {
        return master;
    }

    public boolean isSlave() {
        return !isMaster();
    }

    public Replica getReplica() {
        return replica;
    }
    public MySQLCollationIndex getCollationIndex() {
        return collationIndex;
    }
    public void doHeartbeat() {

    }
}
