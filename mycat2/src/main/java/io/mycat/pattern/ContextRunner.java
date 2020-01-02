package io.mycat.pattern;

import io.mycat.MySQLTaskUtil;
import io.mycat.SQLExecuterWriter;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.calcite.CalciteEnvironment;
import io.mycat.calcite.MetadataManager;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.datasource.jdbc.thread.GProcess;
import io.mycat.lib.impl.JdbcLib;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.MycatSession;
import org.apache.calcite.jdbc.CalciteConnection;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ContextRunner {

    //inst type
    public static final String QUERY = "query";
    public static final String USE_STATEMENT = "useStatement";
    public static final String PROXY_DATASOURCE = "proxyDatasource";
    public static final String PROXY_REPLICA = "proxyReplica";
    public static final String JDBC_DATASOURCE = "jdbcDatasource";
    public static final String SET_TRANSACTION_TYPE = "setTransactionType";


    //item
    public static final String PROXY_TRANSACTION_TYPE = "proxy";
    public static final String JDBC_TRANSACTION_TYPE = "jdbc";
    public static final String SCHEMA_NAME = "schemaName";
    public static final String DATASOURCE_NAME = "datasourceName";
    public static final String REPLICA_NAME = "replicaName";
    public static final String TRANSACTION_TYPE = "transactionType";


    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(ContextRunner.class);

    public List<String> explain() {
        return Collections.singletonList(Objects.toString(this));
    }

    public static void run(MycatClient client, Context context, MycatSession session) {

        String transactionType = client.getTransactionType();
        if (PROXY_TRANSACTION_TYPE.equals(transactionType)) {
            if (session.isBindMySQLSession()) {
                MySQLTaskUtil.proxyBackend(session, context.getSql());
                return;
            }
        }
        String type = context.getType();
        switch (type) {
            case USE_STATEMENT: {
                String schemaName = Objects.requireNonNull(context.getVariable(SCHEMA_NAME));
                client.useSchema(schemaName);
                break;
            }
            case PROXY_DATASOURCE: {
                MySQLTaskUtil.proxyBackendByDatasourceName(session, context.getSql(), context.getVariable(DATASOURCE_NAME));
                break;
            }
            case PROXY_REPLICA: {
                MySQLTaskUtil.proxyBackendByReplicaName(session, context.getSql(), context.getVariable(REPLICA_NAME));
                break;
            }
            case SET_TRANSACTION_TYPE: {
                client.useTransactionType(context.getVariable(TRANSACTION_TYPE));
                session.writeOkEndPacket();
                break;
            }
            case JDBC_DATASOURCE: {
                String sql = context.getSql();
                String datasourceName = context.getVariable(DATASOURCE_NAME);
                JdbcLib.responseQueryOnJdbcByDataSource(datasourceName, sql).apply(session);
                return;
            }
            case QUERY: {
                block(session, new Runner() {
                    @Override
                    public void accept(MycatSession mycat) throws Exception {
                        CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);
                        SQLExecuterWriter.executeQuery(connection, context.getSql());
                    }
                });
                break;
            }
        }
    }

    public static void block(MycatSession mycat, Runner runner) {
        JdbcRuntime.INSTANCE.run(mycat, new GProcess() {
            @Override
            public void accept(BindThreadKey key, TransactionSession session) {
                try {
                    mycat.deliverWorkerThread((SessionThread) Thread.currentThread());
                    runner.accept(mycat);
                } catch (Exception e) {
                    onException(mycat, e);
                } finally {
                    mycat.backFromWorkerThread();
                }
            }

            @Override
            public void onException(BindThreadKey key, Exception e) {
                LOGGER.error("", e);
                mycat.setLastMessage(e.toString());
                mycat.writeErrorEndPacket();
            }
        });
    }

    public static interface Runner {
        void accept(MycatSession mycat) throws Exception;
    }

}