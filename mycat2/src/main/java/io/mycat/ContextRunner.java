/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat;

import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.metadata.MetadataManager;
import io.mycat.calcite.prepare.PlanRunner;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.lib.impl.JdbcLib;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ResultSetProvider;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.sqldb.SqldbRepl;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.mycat.SQLExecuterWriter.writeToMycatSession;

/**
 * @author chen junwen
 */
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
    public static final String ERROR = "error";
    public static final String EXPLAIN = "explain";
    public static final String DISTRIBUTED_QUERY = ("distributedQuery");
    public static final String EXECUTE_PLAN = ("plan");
    public static final String USE_STATEMENT = ("useStatement");
    public static final String COMMIT = ("commit");
    public static final String BEGIN = ("begin");
    public static final String SET_TRANSACTION_ISOLATION = ("setTransactionIsolation");
    public static final String ROLLBACK = ("rollback");
    public static final String EXECUTE = ("execute");
    //    public static final String PASS = ("pass");
    //    public static final String SET_TRANSACTION_TYPE = ("setTransactionType");
    public static final String ON_XA = ("onXA");
    public static final String OFF_XA = ("offXA");
    public static final String SET_AUTOCOMMIT_OFF = ("setAutoCommitOff");
    public static final String SET_AUTOCOMMIT_ON = ("setAutoCommitOn");
    static final ConcurrentHashMap<String, Command> COMMANDS;

    public static void run(MycatClient client, Context analysis, MycatSession session) {
        Command command = Objects.requireNonNull(COMMANDS.getOrDefault(analysis.getCommand(), ERROR_COMMAND));
        Runnable apply = command.apply(client, analysis, session);
        apply.run();
    }


    @ToString
    static class Details {
        ExecuteType executeType;
        Map<String, List<String>> targets;
        String balance;

        public Details(ExecuteType executeType, Map<String, List<String>> backendTableInfos, String balance) {
            this.executeType = executeType;
            this.targets = backendTableInfos;
            this.balance = balance;
        }

        public List<String> toExplain() {
            ArrayList<String> list = new ArrayList<>();
            list.add("executeType = " + executeType);
            for (Map.Entry<String, List<String>> stringListEntry : targets.entrySet()) {
                for (String s : stringListEntry.getValue()) {
                    list.add("target: " + stringListEntry.getKey() + " sql:" + s);
                }
                list.add("balance = " + balance);
            }
            return list;
        }
    }

    public static final Command ERROR_COMMAND = new Command() {
        @Override
        public Runnable apply(MycatClient client, Context context, MycatSession session) {
            String errorMessage = context.getVariable("errorMessage", "may be unknown command");
            int errorCode = Integer.parseInt(context.getVariable("errorCode", "-1"));
            return new Runnable() {
                @Override
                public void run() {
                    session.setLastMessage(errorMessage);
                    session.setLastErrorCode(errorCode);
                    session.writeErrorEndPacketBySyncInProcessError();
                }
            };
        }

        @Override
        public Runnable explain(MycatClient client, Context context, MycatSession session) {
            String errorMessage = context.getVariable("errorMessage", "may be unknown command");
            int errorCode = Integer.parseInt(context.getVariable("errorCode", "-1"));
            return () -> {
                writePlan(session, MessageFormat.format("errorMessage:{0} errorCode:{1}", errorMessage, errorCode));
            };
        }
    };


    static {
        COMMANDS = new ConcurrentHashMap<>();
        COMMANDS.put(ERROR, ERROR_COMMAND);
        /**
         * 参数:statement
         */
        COMMANDS.put(EXPLAIN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                String sql = context.getVariable("statement");
                Context analysis = client.analysis(sql);
                Command command = COMMANDS.get(analysis.getCommand());
                return command.explain(client, analysis, session);
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
        COMMANDS.put(DISTRIBUTED_QUERY, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> block(session, mycat -> {
                    String defaultSchema = client.getDefaultSchema();
                    String explain = context.getExplain().trim();
                    if (explain.endsWith(";")) {
                        explain = explain.substring(0, explain.length() - 1);
                    }
                    LOGGER.debug("session id:{} action: plan {}", session.sessionId(), explain);
                    PlanRunner query = SqldbRepl.INSTANCE.querySQL(defaultSchema, explain);
                    TextResultSetResponse connection = new TextResultSetResponse(query.run(MycatCalciteContext.INSTANCE.create()).get());
                    SQLExecuterWriter.writeToMycatSession(mycat, new MycatResponse[]{connection});
                    TransactionSessionUtil.afterDoAction();//移除已经关闭的连接,
                });
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {

                String defaultSchema = client.getDefaultSchema();
                String command = context.getExplain();

                return () -> block(session, mycat -> {
                    PlanRunner query = SqldbRepl.INSTANCE.querySQL(defaultSchema, command);
                    List<String> explain = query.explain();
                    writePlan(session, explain);
                });
            }
        });
//
//        /**
//         * 参数:接收的sql
//         */
//        COMMANDS.put(EXECUTE_PLAN, new Command() {
//            @Override
//            public Runnable apply(MycatClient client, Context context, MycatSession session) {
//                return () -> {
//                    block(session, mycat -> {
//                        try (CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);) {
//                            connection.setSchema(client.getDefaultSchema());
//                            final FrameworkConfig config = Frameworks.newConfigBuilder()
//                                    .defaultSchema(connection.getRootSchema()).build();
//                            DesRelNodeHandler desRelNodeHandler = new DesRelNodeHandler(config);
//                            RelRunner runner = connection.unwrap(RelRunner.class);
//                            String explain = context.getExplain();
//                            PreparedStatement prepare = runner.prepare(desRelNodeHandler.handle(explain));
//                            LOGGER.debug("session id:{} action: plan {}", session.sessionId(), explain);
//                            writeToMycatSession(session, new MycatResponse[]{new TextResultSetResponse(new JdbcRowBaseIteratorImpl(prepare, prepare.executeQuery()))});
//                            TransactionSessionUtil.afterDoAction();
//                        }
//                    });
//                };
//            }
//
//            @Override
//            public Runnable explain(MycatClient client, Context context, MycatSession session) {
//                return () -> {
//                    block(session, mycat -> {
//                        try (CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);) {
//                            connection.setSchema(client.getDefaultSchema());
//                            final FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(connection.getRootSchema()).build();
//                            writePlan(session, RelOptUtil.toString(new DesRelNodeHandler(config).handle(context.getExplain())));
//                            TransactionSessionUtil.afterDoAction();
//                        }
//                    });
//                    return;
//                };
//            }
//        });

        /**
         * 参数:接收的sql
         */
//        COMMANDS.put(EXECUTE_PLAN, new Command() {
//            @Override
//            public Runnable apply(MycatClient client, Context context, MycatSession session) {
//                return () -> {
//                    block(session, mycat -> {
//                        try (CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);) {
//                            connection.setSchema(client.getDefaultSchema());
//                            final FrameworkConfig config = Frameworks.newConfigBuilder()
//                                    .defaultSchema(connection.getRootSchema()).build();
//                            DesRelNodeHandler desRelNodeHandler = new DesRelNodeHandler(config);
//                            RelRunner runner = connection.unwrap(RelRunner.class);
//                            String explain = context.getExplain();
//                            PreparedStatement prepare = runner.prepare(desRelNodeHandler.handle(explain));
//                            LOGGER.debug("session id:{} action: plan {}", session.sessionId(), explain);
//                            writeToMycatSession(session, new MycatResponse[]{new TextResultSetResponse(new JdbcRowBaseIteratorImpl(prepare, prepare.executeQuery()))});
//                            TransactionSessionUtil.afterDoAction();
//                        }
//                    });
//                };
//            }
//
//            @Override
//            public Runnable explain(MycatClient client, Context context, MycatSession session) {
//                return () -> {
//                    block(session, mycat -> {
//                        try (CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection(MetadataManager.INSTANCE);) {
//                            connection.setSchema(client.getDefaultSchema());
//                            final FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(connection.getRootSchema()).build();
//                            writePlan(session, RelOptUtil.toString(new DesRelNodeHandler(config).handle(context.getExplain())));
//                            TransactionSessionUtil.afterDoAction();
//                        }
//                    });
//                    return;
//                };
//            }
//        });

        /**
         * 参数:SCHEMA_NAME
         */
        COMMANDS.put(USE_STATEMENT, new Command() {
                    @Override
                    public Runnable apply(MycatClient client, Context context, MycatSession session) {
                        return () -> {
                            String schemaName = Objects.requireNonNull(context.getVariable(SCHEMA_NAME));
                            client.useSchema(schemaName);
                            session.setSchema(schemaName);
                            LOGGER.debug("session id:{} action: use {}", session.sessionId(), schemaName);
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
         * 参数:无
         */
        COMMANDS.put(ON_XA, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    if (session.isInTransaction()) throw new IllegalArgumentException();
                    client.useTransactionType(TransactionType.JDBC_TRANSACTION_TYPE);
                    LOGGER.debug("session id:{} action:{}", session.sessionId(), "set xa = 1 exe success");
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
        COMMANDS.put(OFF_XA, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    if (session.isInTransaction()) throw new IllegalArgumentException();
                    client.useTransactionType(TransactionType.PROXY_TRANSACTION_TYPE);
                    LOGGER.debug("session id:{} action:{}", session.sessionId(), "set xa = 0 exe success");
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
        COMMANDS.put(SET_AUTOCOMMIT_OFF, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setAutoCommit(false);
                    LOGGER.debug("session id:{} action:set autocommit = 0 exe success", session.sessionId());
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
        COMMANDS.put(SET_AUTOCOMMIT_ON, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setAutoCommit(true);
                    LOGGER.debug("session id:{} action:set autocommit = 1 exe success", session.sessionId());
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
        COMMANDS.put(BEGIN, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    session.setInTranscation(true);
                    LOGGER.debug("session id:{} action:{}", session.sessionId(), "begin exe success");
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
        COMMANDS.put(SET_TRANSACTION_ISOLATION, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();
                    MySQLIsolation mySQLIsolation = MySQLIsolation.parse(Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                    session.setIsolation(mySQLIsolation);
                    LOGGER.debug("session id:{} action: set isolation = {}", session.sessionId(), mySQLIsolation);
                    session.writeOkEndPacket();
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, Objects.requireNonNull(context.getVariable(TRANSACTION_ISOLATION)));
                };
            }
        });
        /**
         * 参数:无
         */
        COMMANDS.put(ROLLBACK, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();

                    switch (transactionType) {
                        case PROXY_TRANSACTION_TYPE:
                            if (session.isBindMySQLSession()) {
                                session.setInTranscation(false);
                                MySQLTaskUtil.proxyBackend(session, "ROLLBACK");
                                LOGGER.debug("session id:{} action: rollback from binding session", session.sessionId());
                                return;
                            } else {
                                session.setInTranscation(false);
                                session.writeOkEndPacket();
                                LOGGER.debug("session id:{} action: rollback from unbinding session", session.sessionId());
                                return;
                            }
                        case JDBC_TRANSACTION_TYPE:
                            block(session, mycat -> {
                                TransactionSessionUtil.rollback();
                                session.setInTranscation(false);
                                LOGGER.debug("session id:{} action: rollback from xa", session.sessionId());
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
        COMMANDS.put(COMMIT, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    TransactionType transactionType = client.getTransactionType();
                    switch (transactionType) {
                        case PROXY_TRANSACTION_TYPE:
                            if (!session.isBindMySQLSession()) {
                                session.setInTranscation(false);
                                LOGGER.debug("session id:{} action: commit from unbinding session", session.sessionId());
                                session.writeOkEndPacket();
                                return;
                            } else {
                                session.setInTranscation(false);
                                MySQLTaskUtil.proxyBackend(session, "COMMIT");
                                LOGGER.debug("session id:{} action: commit from binding session", session.sessionId());
                                return;
                            }
                        case JDBC_TRANSACTION_TYPE:
                            block(session, mycat -> {
                                TransactionSessionUtil.commit();
                                LOGGER.debug("session id:{} action: commit from xa", session.sessionId());
                                session.setInTranscation(false);
                                mycat.writeOkEndPacket();
                            });
                    }
                };
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                return () -> {
                    writePlan(session, "COMMIT");
                };
            }
        });
        /**
         * 参数:
         * balance
         * targets
         * executeType:
         * metaData:true:false
         * forceProxy:true:false
         * needTransaction:true|false
         */
        COMMANDS.put(EXECUTE, new Command() {
            @Override
            public Runnable apply(MycatClient client, Context context, MycatSession session) {
                boolean forceProxy = "true".equalsIgnoreCase(context.getVariable("forceProxy", "false"));
                boolean needTransactionConfig = "true".equalsIgnoreCase(context.getVariable("needTransaction", "true"));
                boolean needStartTransaction = needTransactionConfig && (!session.isAutocommit() || session.isInTransaction());
                boolean metaData = "true".equalsIgnoreCase(context.getVariable("metaData", "false"));

                final Details details;
                details = getDetails(context, needStartTransaction, metaData);
                Map<String, List<String>> tasks = details.targets;
                String balance = details.balance;
                ExecuteType executeType = details.executeType;
                MySQLIsolation isolation = session.getIsolation();

                LOGGER.debug("session id:{} execute :{}", session.sessionId(), details.toString());
                boolean runOnProxy = isOne(tasks) && client.getTransactionType() == TransactionType.PROXY_TRANSACTION_TYPE || forceProxy;
                if (runOnProxy) {
                    if (tasks.size() != 1) throw new IllegalArgumentException();
                    String[] strings = checkThenGetOne(tasks);
                    return () -> {
                        MySQLTaskUtil.proxyBackendByTargetName(session, strings[0], strings[1],
                                MySQLTaskUtil.TransactionSyncType.create(session.getAutoCommit(), session.isInTransaction()),
                                session.getIsolation(), details.executeType.isMaster(), balance);
                    };
                } else {
                    return () -> {
                        block(session, mycat -> {
                                    if (needStartTransaction) {
                                        LOGGER.debug("session id:{} startTransaction", session.sessionId());
                                        // TransactionSessionUtil.reset();
                                        TransactionSessionUtil.setIsolation(isolation.getJdbcValue());
                                        TransactionSessionUtil.begin();
                                        session.setInTranscation(true);
                                    }
//                                    else if (!session.isInTransaction()) {
//                                        TransactionSessionUtil.reset();
//                                    }
                                    switch (executeType) {
//                                        case RANDOM_QUERY:
                                        case QUERY_MASTER:
                                        case QUERY: {
                                            Map<String, List<String>> backendTableInfos = details.targets;
                                            String[] infos = checkThenGetOne(backendTableInfos);
                                            writeToMycatSession(session, TransactionSessionUtil.executeQuery(infos[0], infos[1]));
                                            return;
                                        }

                                        case INSERT:
                                        case UPDATE:
//                                        case BROADCAST_UPDATE:
                                            writeToMycatSession(session, TransactionSessionUtil.executeUpdateByDatasouce(tasks, true));
                                            return;
                                    }
                                    throw new IllegalArgumentException();
                                }
                        );
                    };
                }
            }

            @Override
            public Runnable explain(MycatClient client, Context context, MycatSession session) {
                boolean metaData = "true".equalsIgnoreCase(context.getVariable("metaData", "false"));
                return () -> writePlan(session, getDetails(context, session.isInTransaction(), metaData).toExplain());
            }
        });
    }

    @NotNull
    private static Details getDetails(Context context, boolean needStartTransaction, boolean metaData) {
        String explain = context.getExplain();//触发注解解析并缓存
        Details details;
        if (metaData) {
            details = getDetails(needStartTransaction, context);
        } else {
            String balance = context.getVariable(BALANCE);
            ExecuteType executeType = ExecuteType.valueOf(context.getVariable(EXECUTE_TYPE, ExecuteType.DEFAULT.name()));
            String replicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(
                    Objects.requireNonNull(context.getVariable(TARGETS), "can not get " + TARGETS + " of " + context.getName()),
                    needStartTransaction, balance);
            details = new Details(executeType, Collections.singletonMap(replicaName, Collections.singletonList(explain)), balance);
        }
        return details;
    }

    private static String[] checkThenGetOne(Map<String, List<String>> backendTableInfos) {
        if (backendTableInfos.size() != 1) {
            throw new IllegalArgumentException();
        }
        Map.Entry<String, List<String>> next = backendTableInfos.entrySet().iterator().next();
        List<String> list = next.getValue();
        if (list.size() != 1) {
            throw new IllegalArgumentException();
        }
        return new String[]{next.getKey(), list.get(0)};
    }

    private static boolean isOne(Map<String, List<String>> backendTableInfos) {
        if (backendTableInfos.size() != 1) {
            return false;
        }
        Map.Entry<String, List<String>> next = backendTableInfos.entrySet().iterator().next();
        List<String> list = next.getValue();
        return list.size() == 1;
    }

    private static void writePlan(MycatSession session, String message) {
        writePlan(session, Arrays.asList(message));
    }

    private static void writePlan(MycatSession session, List<String> messages) {
        MycatResultSet defaultResultSet = ResultSetProvider.INSTANCE.createDefaultResultSet(1, 33, Charset.defaultCharset());
        defaultResultSet.addColumnDef(0, "plan", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
        messages.stream().map(i -> i.replaceAll("\n", " ")).forEach(defaultResultSet::addTextRowPayload);
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
        QUERY_MASTER(true),
        INSERT(true),
        UPDATE(true),
//        RANDOM_QUERY(false),
//        BROADCAST_UPDATE(true),
        ;
        private boolean master;

        public static ExecuteType DEFAULT = ExecuteType.QUERY;

        ExecuteType(boolean master) {
            this.master = master;
        }

    }

    private static Details getDetails(boolean needStartTransaction, Context context) {
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
//        String command = context.getExplain();

        Map<String, List<String>> mid = Collections.emptyMap();
        boolean master = executeType != ExecuteType.QUERY || needStartTransaction;
        switch (executeType) {
            case INSERT:
                Iterable<Map<String, List<String>>> iterable = MetadataManager.INSTANCE.routeInsert(schemaName, context.getExplain());
                Stream<Map<String, List<String>>> stream = StreamSupport.stream(iterable.spliterator(), false);
//                Map<String, List<String>> collect2 = stream.flatMap(i -> i.entrySet().stream())
//                        .collect(Collectors.groupingBy(k -> k.getKey(), Collectors.flatMapping(i -> i.getValue().stream(),Collectors.toList())));JDK9
                Map<String, List<String>> collect = stream.flatMap(i -> i.entrySet().stream())
                        .collect(Collectors.groupingBy(k -> k.getKey(), Collectors.mapping(i -> i.getValue(), Collectors.reducing(new ArrayList<>(), (list, list2) -> {
                            list.addAll(list2);
                            return list;
                        }))));
                mid = collect;
                break;
            case QUERY:
            case QUERY_MASTER:
            case UPDATE: {
                mid = MetadataManager.INSTANCE.rewriteSQL(schemaName, context.getExplain());
                break;
            }
//            case RANDOM_QUERY: {
//                String[] split = SplitUtil.split(context.getVariable(TARGETS), ",");
//                int i = ThreadLocalRandom.current().nextInt(0, split.length);
//                mid = Collections.singletonMap(split[i], command);
//                break;
//            }
//            case BROADCAST_UPDATE: {
//                context.getVariable("table");
//                HashMap<String, String> sqls = new HashMap<>();
//                for (String target : SplitUtil.split(context.getVariable(TARGETS), ",")) {
//                    sqls.put(target, command);
//                }
//                mid = sqls;
//                break;
//            }
        }
        HashMap<String, List<String>> map = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : mid.entrySet()) {
            String datasourceNameByReplicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey(), master, balance);
            List<String> list = map.computeIfAbsent(datasourceNameByReplicaName, s -> new ArrayList<>(1));
            list.addAll(entry.getValue());
        }
        return new Details(executeType, map, balance);
    }
}