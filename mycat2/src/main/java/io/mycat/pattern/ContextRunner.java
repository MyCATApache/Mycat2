package io.mycat.pattern;

import io.mycat.DesRelNodeHandler;
import io.mycat.MySQLTaskUtil;
import io.mycat.SQLExecuterWriter;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.calcite.CalciteEnvironment;
import io.mycat.calcite.MetadataManager;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.lib.impl.JdbcLib;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ResultSetProvider;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.SplitUtil;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelRunner;

import java.nio.charset.Charset;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.mycat.SQLExecuterWriter.writeToMycatSession;

public class ContextRunner {

    public static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ContextRunner.class);
    //item
    public static final String SCHEMA_NAME = "schema";
    public static final String TARGETS = "targets";
    public static final String TRANSACTION_TYPE = "transactionType";
    public static final String EXECUTE_TYPE = "executeType";
    public static final String TRANSACTION_ISOLATION = "transactionIsolation";
    public static final String BALANCE = "balance";

    //inst command
    public static final String EXPLAIN = "explain";
    public static final String SELECT = ("select");
    public static final String EXECUTE_PLAN = ("plan");
    public static final String USE_STATEMENT = ("useStatement");
    public static final String COMMIT = ("commit");
    public static final String BEGIN = ("begin");
    public static final String SET_TRANSACTION_ISOLATION = ("setTransactionIsolation");
    public static final String ROLLBACK = ("rollback");
    public static final String EXECUTE = ("execute");
    public static final String PASS = ("pass");
    public static final String SET_TRANSACTION_TYPE = ("setTransactionType");
    public static final String SET_AUTOCOMMIT_OFF = ("setAutoCommitOff");
    public static final String SET_AUTOCOMMIT_ON = ("setAutoCommitOn");
    static final ConcurrentHashMap<String, Command> map;

    public static void run(MycatClient client, Context analysis, MycatSession session) {
        Command command = Objects.requireNonNull(map.get(analysis.getCommand()));
        command.apply(client, analysis, session).run();
    }

    @ToString
    static class Details {
        ExecuteType executeType;
        Map<String, String> backendTableInfos;
        String balance;

        public Details(ExecuteType executeType, Map<String, String> backendTableInfos, String balance) {
            this.executeType = executeType;
            this.backendTableInfos = backendTableInfos;
            this.balance = balance;
        }
    }



    static {
        map = new ConcurrentHashMap<>();
        /**
         * 参数:statement
         */
        map.put(EXPLAIN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                String sql = context.getVariable("statement");
                Context analysis = client.analysis(sql);
                Command command = map.get(analysis.getExplain());
                return command.explain(client, context, session);
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "explain statement");
                };
            }
        });
        /**
         * 参数:接收的sql
         */
        map.put(SELECT, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> block(session, mycat -> {
                    String defaultSchema = client.getDefaultSchema();
                    String command = context.getExplain();
                    CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);
                    connection.setSchema(defaultSchema);
                    SQLExecuterWriter.executeQuery(mycat, connection, command);
                    TransactionSessionUtil.afterDoAction();//移除已经关闭的连接,
                });
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {

                String defaultSchema = client.getDefaultSchema();
                String command = context.getExplain();

                return () -> block(session, mycat -> {
                    CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);
                    connection.setSchema(defaultSchema);
                    final FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(connection.getRootSchema()).build();
                    PlannerImpl planner = new PlannerImpl(config);//执行计划需要进行解析，验证，转换三步
                    SqlNode parse = planner.parse(command);
                    SqlNode validate = planner.validate(parse);
                    RelNode convert = planner.convert(validate);//
                    String message = RelOptUtil.toString(convert);//输出可读的关系表达式
                    connection.close();
                    writePlan(session, message);
                    TransactionSessionUtil.afterDoAction();//移除已经关闭的连接,
                });
            }
        });
        /**
         * 参数:接收的sql
         */
        map.put(EXECUTE_PLAN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    block(session, mycat -> {
                        try (CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);) {
                            connection.setSchema(client.getDefaultSchema());
                            final FrameworkConfig config = Frameworks.newConfigBuilder()
                                    .defaultSchema(connection.getRootSchema()).build();
                            DesRelNodeHandler desRelNodeHandler = new DesRelNodeHandler(config);
                            RelRunner runner = connection.unwrap(RelRunner.class);
                            PreparedStatement prepare = runner.prepare(desRelNodeHandler.handle(context.getExplain()));
                            writeToMycatSession(session, new MycatResponse[]{new TextResultSetResponse(new JdbcRowBaseIteratorImpl(prepare, prepare.executeQuery(), connection))});
                            TransactionSessionUtil.afterDoAction();
                        }
                    });
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    block(session, mycat -> {
                        try (CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);) {
                            connection.setSchema(client.getDefaultSchema());
                            final FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(connection.getRootSchema()).build();
                            writePlan(session, RelOptUtil.toString(new DesRelNodeHandler(config).handle(context.getExplain())));
                            TransactionSessionUtil.afterDoAction();
                        }
                    });
                    return;
                };
            }
        });
        /**
         * 参数:SCHEMA_NAME
         */
        map.put(USE_STATEMENT, new Command() {
                    @Override
                    public Runnable apply(MycatClient client, Context context, MycatSession session) {
                        return () -> {
                            String schemaName = Objects.requireNonNull(context.getVariable(SCHEMA_NAME));
                            client.useSchema(schemaName);
                            session.writeOkEndPacket();
                        };
                    }

                    @Override
                    public Runnable explain(MycatClient client, Context context, MycatSession session) {
                        return () -> {
                            String schemaName = Objects.requireNonNull(context.getVariable(SCHEMA_NAME));
                            writePlan(session, "use " + schemaName);
                        };
                    }
                }
        );
        /**
         * 参数:transactionType
         */
        map.put(SET_TRANSACTION_TYPE, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    if (session.isInTransaction()) throw new IllegalArgumentException();
                    client.useTransactionType(TransactionType.parse(context.getVariable(TRANSACTION_TYPE)));
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, context.getVariable(TRANSACTION_TYPE));
                };
            }
        });
        /**
         * 参数:无
         */
        map.put(SET_AUTOCOMMIT_OFF, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setAutoCommit(false);
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "SET_AUTOCOMMIT_OFF");
                };
            }
        });
        /**
         * 参数:无
         */
        map.put(SET_AUTOCOMMIT_ON, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setAutoCommit(true);
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "SET_AUTOCOMMIT_ON");
                };
            }
        });
        /**
         * 参数:无
         */
        map.put(BEGIN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setInTranscation(true);
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "BEGIN");
                };
            }
        });
        /**
         * 参数:transactionIsolation
         */
        map.put(SET_TRANSACTION_ISOLATION, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();
                    MySQLIsolation mySQLIsolation = MySQLIsolation.valueOf(Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                    session.setIsolation(mySQLIsolation);
                    switch (transactionType) {
                        case PROXY_TRANSACTION_TYPE:
                            MySQLTaskUtil.proxyBackend(session, context.getExplain());
                            return;
                        case JDBC_TRANSACTION_TYPE:
                            block(session, mycat -> {
                                TransactionSessionUtil.setIsolation(mySQLIsolation.getJdbcValue());
                                mycat.writeOkEndPacket();
                            });
                    }
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session,  Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                };
            }
        });
        /**
         * 参数:无
         */
        map.put(ROLLBACK, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();
                    session.setInTranscation(false);
                    switch (transactionType) {
                        case PROXY_TRANSACTION_TYPE:
                            if (session.isBindMySQLSession()) {
                                MySQLTaskUtil.proxyBackend(session, "ROLLBACK");
                                return;
                            } else {
                                session.writeOkEndPacket();
                                return;
                            }
                        case JDBC_TRANSACTION_TYPE:
                            block(session, mycat -> {
                                TransactionSessionUtil.rollback();
                                mycat.writeOkEndPacket();
                            });
                            return;
                    }
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "ROLLBACK");
                };
            }
        });
        /**
         * 参数:无
         */
        map.put(COMMIT, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();
                    MySQLIsolation mySQLIsolation = MySQLIsolation.valueOf(Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                    session.setIsolation(mySQLIsolation);
                    session.setInTranscation(false);
                    switch (transactionType) {
                        case PROXY_TRANSACTION_TYPE:
                            if (!session.isBindMySQLSession()) {
                                session.writeOkEndPacket();
                                return;
                            } else {
                                MySQLTaskUtil.proxyBackend(session, "COMMIT");
                                LOGGER.debug("proxy commit");
                                return;
                            }
                        case JDBC_TRANSACTION_TYPE:
                            block(session, mycat -> {
                                TransactionSessionUtil.commit();
                                mycat.writeOkEndPacket();
                            });
                    }
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> { writePlan(session, "COMMIT"); };
            }
        });
        /**
         * 参数:
         * type:proxy|jdbc
         * balance
         * targets 一个目标
         * update:true|false
         * needGeneratedKeys:true|false
         */
        map.put(PASS, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                boolean isProxy = "proxy".equalsIgnoreCase(context.getVariable("type", "proxy"));
                String balance = context.getVariable(BALANCE);
                String targets = context.getVariable(TARGETS);
                boolean isUpdate = "true".equalsIgnoreCase(context.getVariable("update"));
                boolean needGeneratedKeys = "true".equalsIgnoreCase(context.getVariable("needGeneratedKeys"));
                String command = context.getExplain();
                return () -> {
                    if (isProxy) {
                        MySQLTaskUtil.proxyBackendByTargetName(session, targets, context.getExplain(), false, MySQLIsolation.DEFAULT, isUpdate, balance);
                    } else {
                        String datasourceName = Objects.requireNonNull(ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(targets, isUpdate, balance));
                        block(session, mycat -> {
                            writeToMycatSession(session, isUpdate ? TransactionSessionUtil.executeQuery(datasourceName, command) : TransactionSessionUtil.executeUpdate(datasourceName, command, needGeneratedKeys));
                        });
                    }
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> writePlan(session, context.toString());
            }
        });
        /**
         * 参数:
         * type:proxy|jdbc
         * balance
         * targets 一个目标
         * update:true|false
         * needGeneratedKeys:true|false
         * pass:true|false
         */
        map.put(EXECUTE, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                boolean needStartTransaction = !session.isAutocommit() || session.isInTransaction();
                Details details = getDetails(context);
                Map<String, String> tasks = details.backendTableInfos;
                String balance = details.balance;
                ExecuteType executeType = details.executeType;
                MySQLIsolation isolation = session.getIsolation();
                switch (client.getTransactionType()) {
                    case PROXY_TRANSACTION_TYPE: {
                        if (tasks.size() != 1) throw new IllegalArgumentException();
                        return () -> {
                            for (Map.Entry<String, String> entry : tasks.entrySet()) {
                                MySQLTaskUtil.proxyBackendByTargetName(session, entry.getKey(), entry.getValue(), needStartTransaction && !session.isBindMySQLSession(), session.getIsolation(), details.executeType.isMaster(), balance);
                                return;
                            }
                        };
                    }
                    case JDBC_TRANSACTION_TYPE: {
                        return () -> { block(session, mycat -> {
                                        TransactionSessionUtil.setIsolation(isolation.getJdbcValue());
                                        if (needStartTransaction) {
                                            TransactionSessionUtil.setAutocommitOff();
                                            session.setInTranscation(true);
                                        }
                                        switch (executeType) {
                                            case RANDOM_QUERY:
                                            case QUERY: {
                                                Map<String, String> backendTableInfos = details.backendTableInfos;
                                                if (backendTableInfos.size() != 1) {
                                                    throw new IllegalArgumentException();
                                                }
                                                for (Map.Entry<String, String> entry : backendTableInfos.entrySet()) {
                                                    writeToMycatSession(session, TransactionSessionUtil.executeQuery(entry.getKey(), entry.getValue()));
                                                    return;
                                                }
                                            }
                                            case INSERT:
                                            case UPDATE:
                                            case BROADCAST_UPDATE:
                                            case INSERT_INSERTID:
                                            case BROADCAST_UPDATEID:
                                            case UPDATE_INSERTID: {
                                                boolean id = false;
                                                switch (executeType) {
                                                    case INSERT_INSERTID:
                                                    case BROADCAST_UPDATEID:
                                                    case UPDATE_INSERTID:
                                                        id = true;
                                                    default:
                                                }
                                                writeToMycatSession(session, TransactionSessionUtil.executeUpdateByDatasouce(tasks, id));
                                                return;
                                            }
                                        }
                                        throw new IllegalArgumentException();
                                    }
                            );
                        };
                    }
                }
                throw new IllegalArgumentException();
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> writePlan(session, getDetails(context).toString());
            }
        });
    }

    private static void writePlan(MycatSession session, String message) {
        MycatResultSet defaultResultSet = ResultSetProvider.INSTANCE.createDefaultResultSet(1, 33, Charset.defaultCharset());
        defaultResultSet.addColumnDef(0, "plan", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
        defaultResultSet.addTextRowPayload(message);
        SQLExecuterWriter.writeToMycatSession(session, defaultResultSet);
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

    @Getter
    enum ExecuteType {
        QUERY(false),
        INSERT(true),
        INSERT_INSERTID(true),
        UPDATE(true),
        UPDATE_INSERTID(true),
        RANDOM_QUERY(false),
        BROADCAST_UPDATE(true),
        BROADCAST_UPDATEID(true),
        ;
        private boolean master;

        ExecuteType(boolean update) {
            this.master = update;
        }
    }

    private static Details getDetails(Context context) {
        Map<String, Collection<String>> tables = context.getTables();
        if (tables.size() != 1) {
            throw new UnsupportedOperationException();
        }
        Map.Entry<String, Collection<String>> next = tables.entrySet().iterator().next();
        if (next.getValue().size() != 1) {
            throw new UnsupportedOperationException();
        }
        String schemaName = next.getKey();
        String tableName = next.getValue().iterator().next();
        ExecuteType executeType = ExecuteType.valueOf(context.getVariable(EXECUTE_TYPE));
        String balance = context.getVariable(BALANCE);
        String command = context.getExplain();

        Details details = null;

        Map<String, String> mid = null;
        switch (executeType) {
            case QUERY:
                mid = Collections.singletonMap(context.getVariable(TARGETS), command);
                break;
            case INSERT:
            case INSERT_INSERTID:
                Iterable<Map<String, String>> iterable = () -> MetadataManager.INSTANCE.routeInsert(schemaName, context.getExplain());
                Stream<Map<String, String>> stream = StreamSupport.stream(iterable.spliterator(), false);
                Map<String, String> collect = stream.flatMap(i -> i.entrySet().stream()).collect(Collectors.groupingBy(k -> k.getKey(), Collectors.mapping(i -> i.getValue(), Collectors.joining(";"))));
                //@todo 跨库 多语句插入?
                mid = collect;
                break;
            case UPDATE:
            case UPDATE_INSERTID: {
                mid = MetadataManager.INSTANCE.rewriteUpdateSQL(schemaName, context.getExplain());
                break;
            }
            case RANDOM_QUERY: {
                String[] split = SplitUtil.split(context.getVariable(TARGETS), ",");
                int i = ThreadLocalRandom.current().nextInt(0, split.length);
                mid = Collections.singletonMap(split[i], command);
                break;
            }
            case BROADCAST_UPDATE:
            case BROADCAST_UPDATEID: {
                HashMap<String, String> sqls = new HashMap<>();
                for (String target : SplitUtil.split(context.getVariable(TARGETS), ",")) {
                    sqls.put(target, command);
                }
                mid = sqls;
                break;
            }
        }
        HashMap<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : mid.entrySet()) {
            String datasourceNameByReplicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey(), true, balance);
            if (map.containsKey(datasourceNameByReplicaName)) {
                throw new IllegalArgumentException("最终获得的数据源重复");
            } else {
                map.put(datasourceNameByReplicaName, entry.getValue());
            }
        }
        return new Details(executeType, map, balance);
    }
}