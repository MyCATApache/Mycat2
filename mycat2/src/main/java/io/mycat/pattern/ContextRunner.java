package io.mycat.pattern;

import io.mycat.DesRelNodeHandler;
import io.mycat.MySQLTaskUtil;
import io.mycat.MycatException;
import io.mycat.SQLExecuterWriter;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.calcite.CalciteEnvironment;
import io.mycat.calcite.MetadataManager;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.lib.impl.JdbcLib;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.SplitUtil;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.sql.PreparedStatement;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ContextRunner {

  public  static  final   MycatLogger LOGGER = MycatLoggerFactory.getLogger(ContextRunner.class);
    //inst type
    //item
    public static final String PROXY_TRANSACTION_TYPE = "proxy";
    public static final String JDBC_TRANSACTION_TYPE = "jdbc";
    public static final String SCHEMA_NAME = "schema";
    public static final String TARGETS = "target";
    public static final String TRANSACTION_TYPE = "transactionType";
    public static final String TRANSACTION_ISOLATION = "transactionIsolation";

    public static final String MESSAGE = "Unknown transaction status";

    public static final HashMap<String, Action> map = new HashMap<>();

    static {
        for (Action value : Action.values()) {
            map.put(value.name, value);
        }

    }

    public List<String> explain() {
        return Collections.singletonList(Objects.toString(this));
    }

    public static void run(MycatClient client, Context context, MycatSession session) {
        String command = context.getCommand();
        String transactionType = Objects.requireNonNull(client.getTransactionType() != null ? client.getTransactionType() : MetadataManager.INSTANCE.getDefaultTransactionType());
        MySQLIsolation isolation = session.getIsolation();
        String type = context.getType();
        Action action = map.get(type);
        switch (action) {
            case EXPLAIN: {
                break;
            }
            case SELECT: {
                block(session, mycat -> {
                    CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);
                    SQLExecuterWriter.executeQuery(mycat, connection, context.getCommand());
                    TransactionSessionUtil.afterDoAction();
                });
                return;
            }
            case UPDATE:
            case DELETE: {
                Map<String, Collection<String>> tables = context.getTables();
                if (tables.size() == 1) {
                    for (Map.Entry<String, Collection<String>> stringCollectionEntry : tables.entrySet()) {
                        String schemaName = stringCollectionEntry.getKey();
                        ExecuteType executeType = ExecuteType.valueOf(context.getVariable("executeType"));
                        switch (executeType) {
                            case QUERY:
                                break;
                            case UPDATE:
                            case UPDATE_INSERTID: {
                                Map<String, String> backendTableInfoStringMap = MetadataManager.INSTANCE.rewriteUpdateSQL(schemaName, context.getCommand());
                                normal(session, transactionType, isolation, executeType, backendTableInfoStringMap);
                                return;
                            }
                            case GLOBAL_UPDATE:
                            case GLOBAL_UPDATEID: {
                                HashMap<String, String> sqls = new HashMap<>();
                                String[] targets = SplitUtil.split(context.getVariable("targets"), ",");
                                for (String target : targets) {
                                    sqls.put(target, command);
                                }
                                normal(session, transactionType, isolation, executeType, sqls);
                                return;
                            }
                        }
                    }
                }
                throw new UnsupportedOperationException();
            }
            case INSERT: {
                Map<String, Collection<String>> tables = context.getTables();
                if (tables.size() == 1) {
                    for (Map.Entry<String, Collection<String>> stringCollectionEntry : tables.entrySet()) {
                        String schemaName = stringCollectionEntry.getKey();
                        ExecuteType executeType = ExecuteType.valueOf(context.getVariable("executeType"));
                        switch (executeType) {
                            case QUERY:
                                break;
                            case UPDATE:
                            case UPDATE_INSERTID: {
                                Iterable<Map<String, String>> iterable = () -> MetadataManager.INSTANCE.routeInsert(schemaName, context.getCommand());
                                Stream<Map<String, String>> stream = StreamSupport.stream(iterable.spliterator(), false);
                                Map<String, String> collect = stream.flatMap(i -> i.entrySet().stream()).collect(Collectors.groupingBy(k -> k.getKey(), Collectors.mapping(i -> i.getValue(), Collectors.joining(";"))));
                                normal(session, transactionType, isolation, executeType, collect);
                                return;
                            }
                            case GLOBAL_UPDATE:
                            case GLOBAL_UPDATEID: {
                                HashMap<String, String> sqls = new HashMap<>();
                                String[] targets = SplitUtil.split(context.getVariable("targets"), ",");
                                for (String target : targets) {
                                    sqls.put(target, command);
                                }
                                normal(session, transactionType, isolation, executeType, sqls);
                                return;
                            }
                        }
                    }
                }
                throw new UnsupportedOperationException();
            }
            case HBT: {
                block(session, mycat -> {
                    final SchemaPlus rootSchema = Frameworks.createRootSchema(false);
                    CalciteEnvironment.INSTANCE.init(rootSchema, MetadataManager.INSTANCE);
                    final FrameworkConfig config = Frameworks.newConfigBuilder()
                            .defaultSchema(rootSchema).build();
                    DesRelNodeHandler desRelNodeHandler = new DesRelNodeHandler(config);
                    PreparedStatement handle = desRelNodeHandler.handle(command);
                    SQLExecuterWriter.writeToMycatSession(mycat, SQLExecuterWriter.getMycatResponses(handle, handle.getResultSet()));
                });
                return;
            }
            case USE_STATEMENT: {
                String schemaName = Objects.requireNonNull(context.getVariable(SCHEMA_NAME));
                client.useSchema(schemaName);
                session.writeOkEndPacket();
                return;
            }

            case SET_TRANSACTION_TYPE: {
                if (session.isInTransaction()) {
                    throw new IllegalArgumentException();
                }
                client.useTransactionType(context.getVariable(TRANSACTION_TYPE));
                session.writeOkEndPacket();
                return;
            }
            case SET_PROXY_ON: {
                if (session.isInTransaction()) {
                    throw new IllegalArgumentException();
                }
                client.useTransactionType(PROXY_TRANSACTION_TYPE);
                session.writeOkEndPacket();
                return;
            }
            case SET_XA_ON: {
                if (session.isInTransaction()) {
                    throw new IllegalArgumentException();
                }
                client.useTransactionType(JDBC_TRANSACTION_TYPE);
                session.writeOkEndPacket();
                return;
            }
            case SET_AUTOCOMMIT_OFF: {
                session.setAutoCommit(false);
                session.writeOkEndPacket();
                return;
            }
            case SET_AUTOCOMMIT_ON: {
                session.setAutoCommit(true);
                session.writeOkEndPacket();
                return;
            }
            case BEGIN: {
                session.setInTranscation(true);
                session.writeOkEndPacket();
                return;
            }
            case SET_TRANSACTION_ISOLATION: {
                MySQLIsolation mySQLIsolation = MySQLIsolation.valueOf(Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                session.setIsolation(mySQLIsolation);
                if (PROXY_TRANSACTION_TYPE.equals(transactionType)) {
                    MySQLTaskUtil.proxyBackend(session, context.getCommand());
                    return;
                }
                if (JDBC_TRANSACTION_TYPE.equals(transactionType)) {
                    block(session, mycat -> {
                        TransactionSessionUtil.setIsolation(mySQLIsolation.getJdbcValue());
                        mycat.writeOkEndPacket();
                    });
                    return;
                }
                session.setLastMessage("Unknown transaction status");
                session.writeErrorEndPacketBySyncInProcessError();
                return;
            }
            case ROLLBACK: {
                session.setInTranscation(false);
                if (PROXY_TRANSACTION_TYPE.equals(transactionType)) {
                    MySQLTaskUtil.proxyBackend(session, context.getCommand());
                    return;
                }
                if (JDBC_TRANSACTION_TYPE.equals(transactionType)) {
                    block(session, mycat -> {
                        TransactionSessionUtil.rollback();
                        mycat.writeOkEndPacket();
                    });
                    return;
                }
                session.setLastMessage("Unknown transaction status");
                session.writeErrorEndPacketBySyncInProcessError();
                return;
            }
            case COMMIT: {
                session.setInTranscation(false);
                if (PROXY_TRANSACTION_TYPE.equals(transactionType)) {
                    if (!session.isBindMySQLSession()){
                        session.writeOkEndPacket();
                        return;
                    }
                    MySQLTaskUtil.proxyBackend(session, context.getCommand());
                    LOGGER.debug("proxy commit");
                    return;
                }
                if (JDBC_TRANSACTION_TYPE.equals(transactionType)) {
                    block(session, mycat -> {
                        TransactionSessionUtil.commit();
                        LOGGER.debug("jdbc commit");
                        mycat.writeOkEndPacket();
                    });
                    return;
                }
                session.setLastMessage("Unknown transaction status");
                session.writeErrorEndPacketBySyncInProcessError();
                return;
            }
            case PROXY_ONLY: {
                String tagret = context.getVariable(TARGETS);
                MySQLTaskUtil.proxyBackendByReplicaName(session, tagret,command, false, MySQLIsolation.DEFAULT);
                return;
            }
            case JDBC_QUERY_ONLY: {

                String tagret = context.getVariable(TARGETS);
                String datasourceName = Objects.requireNonNull(ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(tagret));
                SQLExecuterWriter.writeToMycatSession(session, TransactionSessionUtil.executeQuery(datasourceName, command));
                return;
            }
            case EXECUTE:
                ExecuteType executeType = ExecuteType.valueOf(context.getVariable("executeType"));
                String tagret = context.getVariable(TARGETS);
                switch (executeType) {
                    case GLOBAL_UPDATE:
                    case GLOBAL_UPDATEID:
                        HashMap<String, String> res = new HashMap<>();
                        for (String target : SplitUtil.split(tagret, ",")) {
                            res.put(target, command);
                        }
                        normal(session, transactionType, isolation, executeType, res);
                        return;
                }
                normal(session, transactionType, isolation, executeType, tagret, command);
                return;
            case UNKNOWN:
                break;
        }
        throw new UnsupportedOperationException();
    }

    private static void normal(MycatSession session, String transactionType, MySQLIsolation isolation, ExecuteType executeType, String datasourceName, String sql) {
        normal(session, transactionType, isolation, executeType, Collections.singletonMap(datasourceName, sql));
    }

    private static void normal(MycatSession session, String transactionType, MySQLIsolation isolation, ExecuteType executeType, Map<String, String> sqls) {
        boolean needStartTransaction = !session.isAutocommit()||session.isInTransaction();
        if (PROXY_TRANSACTION_TYPE.equals(transactionType)) {
            if (executeType == ExecuteType.GLOBAL_UPDATE || executeType == ExecuteType.GLOBAL_UPDATEID || sqls.size() != 1 || sqls.isEmpty()) {
                throw new IllegalArgumentException();
            } else {
                for (Map.Entry<String, String> entry : sqls.entrySet()) {
                    MySQLTaskUtil.proxyBackendByReplicaName(session, entry.getKey(), entry.getValue(), needStartTransaction && !session.isBindMySQLSession(), isolation);
                    return;
                }
            }
        } else if (JDBC_TRANSACTION_TYPE.equals(transactionType)) {
            switch (executeType) {
                case QUERY:
                case UPDATE:
                case UPDATE_INSERTID:
                    if (sqls.size() != 1 || sqls.isEmpty()) {
                        throw new IllegalArgumentException();
                    }
                    break;
                case GLOBAL_UPDATE:
                case GLOBAL_UPDATEID: {
                    if (sqls.isEmpty()) {
                        throw new IllegalArgumentException();
                    }
                    break;
                }
            }
            block(session, mycat -> {
                TransactionSessionUtil.setIsolation(isolation.getJdbcValue());
                if (needStartTransaction && !JdbcRuntime.INSTANCE.isBindingInTransaction(session)) {
                    TransactionSessionUtil.setAutocommitOff();
                }
                switch (executeType) {
                    case QUERY: {
                        for (Map.Entry<String, String> entry : sqls.entrySet()) {
                            String datasourceName = Objects.requireNonNull(ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey()));
                            SQLExecuterWriter.writeToMycatSession(session, TransactionSessionUtil.executeQuery(datasourceName, entry.getValue()));
                            return;
                        }
                        break;
                    }
                    case UPDATE:
                    case UPDATE_INSERTID: {
                        boolean id = executeType == ExecuteType.UPDATE_INSERTID;
                        for (Map.Entry<String, String> entry : sqls.entrySet()) {
                            String datasourceName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey());
                            SQLExecuterWriter.writeToMycatSession(session, TransactionSessionUtil.executeUpdate(datasourceName, entry.getValue(), id));
                            return;
                        }
                    }
                    break;
                    case GLOBAL_UPDATE:
                    case GLOBAL_UPDATEID: {
                        boolean id = executeType == ExecuteType.GLOBAL_UPDATEID;
                        HashMap<String, String> finalMap = new HashMap<>();
                        for (Map.Entry<String, String> entry : sqls.entrySet()) {
                            String datasourceName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey());
                            finalMap.put(datasourceName, entry.getValue());
                        }
                        SQLExecuterWriter.writeToMycatSession(session, TransactionSessionUtil.executeUpdateByDatasouce(finalMap, id));
                        return;
                    }
                }
            });
        }
        throw new IllegalArgumentException();
    }


    private static void checkTransactionState(String transactionType) {
        if (!JDBC_TRANSACTION_TYPE.equals(transactionType)) {
            throw new MycatException("commit only in JDBC_TRANSACTION_TYPE");
        }
    }

    public static void block(MycatSession mycat, Runner runner) {
        JdbcLib.block(mycat, runner);
    }


    @FunctionalInterface
    public static interface Runner extends Consumer<MycatSession> {
        void on(MycatSession mycat) throws Exception;

        @SneakyThrows
        @Override
        default void accept(MycatSession session) {
            on(session);
        }
    }

    enum ExecuteType {
        QUERY,
        UPDATE,
        UPDATE_INSERTID,
        GLOBAL_UPDATE,
        GLOBAL_UPDATEID,
    }

}