package io.mycat.sqlhandler.dql;

import cn.mycat.vertx.xa.XaLog;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.util.JdbcUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import io.mycat.*;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.ExecutorProvider;
import io.mycat.calcite.PrepareExecutor;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.spm.*;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.commands.MycatdbCommand;
import io.mycat.commands.SqlResultSetService;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.exporter.SqlRecorderRuntime;
import io.mycat.hint.*;
import io.mycat.monitor.MycatSQLLogMonitor;
import io.mycat.monitor.SqlEntry;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelector;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStatus;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import io.mycat.sqlhandler.*;
import io.mycat.sqlhandler.config.StorageManager;
import io.mycat.sqlhandler.dml.UpdateSQLHandler;
import io.mycat.util.JsonUtil;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.mycat.vertx.VertxUpdateExecuter;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class HintHandler extends AbstractSQLHandler<MySqlHintStatement> {

    public static final HintHandler INSTANCE = new HintHandler();

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlHintStatement> request, MycatDataContext dataContext, Response response) {
        MySqlHintStatement ast = request.getAst();
        List<SQLCommentHint> hints = ast.getHints();
        try {
            if (hints.size() == 1) {
                String s = SqlHints.unWrapperHint(hints.get(0).getText());
                if (s.startsWith("mycat:") || s.startsWith("MYCAT:")) {
                    s = s.substring(6);
                    int bodyStartIndex = s.indexOf('{');
                    String cmd;
                    String body;
                    if (bodyStartIndex == -1) {
                        cmd = s;
                        body = "{}";
                    } else {
                        cmd = s.substring(0, bodyStartIndex);
                        body = s.substring(bodyStartIndex);
                    }
                    cmd = cmd.trim();

                    MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                    MycatRouterConfig routerConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
                    ReplicaSelectorManager replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
                    JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    MycatServer mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);

                    if ("showErGroup".equalsIgnoreCase(cmd)) {
                        return showErGroup(response, metadataManager);
                    }
                    if ("loaddata".equalsIgnoreCase(cmd)) {
                        return loaddata(dataContext, response, body, metadataManager);
                    }
                    if ("setUserDialect".equalsIgnoreCase(cmd)) {
                        return setUserDialect(response, body);
                    }
                    if ("showSlowSql".equalsIgnoreCase(cmd)) {
                        return showSlowSql(response, body);
                    }
                    if ("showTopology".equalsIgnoreCase(cmd)) {
                        return showTopology(response, body, metadataManager);
                    }
                    if ("checkConfigConsistency".equalsIgnoreCase(cmd)) {
                        StorageManager assembleMetadataStorageManager = MetaClusterCurrent.wrapper(StorageManager.class);
                        boolean res = assembleMetadataStorageManager.checkConfigConsistency();
                        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
                        resultSetBuilder.addColumnInfo("value", JDBCType.VARCHAR);
                        resultSetBuilder.addObjectRowPayload(Arrays.asList(res ? 1 : 0));
                        return response.sendResultSet(resultSetBuilder.build());
                    }
                    if ("resetConfig".equalsIgnoreCase(cmd)) {
                        MycatRouterConfigOps ops = ConfigUpdater.getOps();
                        ops.reset();
                        ops.commit();
                        return response.sendOk();
                    }
                    if ("run".equalsIgnoreCase(cmd)) {
                        Map<String, Object> map = JsonUtil.from(body, Map.class);
                        String hbt = Objects.toString(map.get("hbt"));
                        DrdsSqlCompiler drdsRunner = MetaClusterCurrent.wrapper(DrdsSqlCompiler.class);
                        Plan plan = drdsRunner.doHbt(hbt);
                        AsyncMycatDataContextImpl.HbtMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.HbtMycatDataContextImpl(dataContext, plan.getCodeExecuterContext());
                        ArrayBindable arrayBindable = MetaClusterCurrent.wrapper(ExecutorProvider.class).prepare(plan).getArrayBindable();
                        Observable<MysqlPayloadObject> mysqlPayloadObjectObservable = PrepareExecutor.getMysqlPayloadObjectObservable(arrayBindable, sqlMycatDataContext, plan.getMetaData());
                        return response.sendResultSet(mysqlPayloadObjectObservable);
                    }
                    if ("killThread".equalsIgnoreCase(cmd)) {
                        KillThreadHint killThreadHint = JsonUtil.from(body, KillThreadHint.class);
                        long pid = killThreadHint.getId();
                        dataContext.setAffectedRows(IOExecutor.kill(pid) ? 1 : 0);
                        return response.sendOk();
                    }
                    if ("interruptThread".equalsIgnoreCase(cmd)) {
                        Thread.currentThread().interrupt();
                        InterruptThreadHint interruptThreadHint = JsonUtil.from(body, InterruptThreadHint.class);
                        long pid = interruptThreadHint.getId();
                        dataContext.setAffectedRows(IOExecutor.interrupt(pid) ? 1 : 0);
                        return response.sendOk();
                    }
                    if ("showThreadInfo".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        builder.addColumnInfo("ID", JDBCType.VARCHAR);
                        builder.addColumnInfo("NAME", JDBCType.VARCHAR);
                        builder.addColumnInfo("STATE", JDBCType.VARCHAR);
                        builder.addColumnInfo("STACKTRACE", JDBCType.VARCHAR);

                        List<Thread> threads = IOExecutor.findAllThreads();
                        for (Thread thread : threads) {
                            String name = thread.getName();
                            long id = thread.getId();
                            String state = thread.getState().name();
                            StackTraceElement[] stackTrace = thread.getStackTrace();

                            StringWriter stringWriter = new StringWriter();
                            for (StackTraceElement traceElement : stackTrace) {
                                stringWriter.write("\tat " + traceElement);
                            }
                            String stackTraceText = stringWriter.toString();

                            builder.addObjectRowPayload(Arrays.asList(id, name, state, stackTraceText));
                        }

                        return response.sendResultSet(builder.build());
                    }
                    if ("createSqlCache".equalsIgnoreCase(cmd)) {
                        MycatRouterConfigOps ops = ConfigUpdater.getOps();
                        SQLStatement sqlStatement = null;
                        if (ast.getHintStatements() != null && ast.getHintStatements().size() == 1) {
                            sqlStatement = ast.getHintStatements().get(0);
                        }
                        SqlCacheConfig sqlCacheConfig = JsonUtil.from(body, SqlCacheConfig.class);
                        if (sqlCacheConfig.getSql() == null && sqlStatement != null) {
                            sqlCacheConfig.setSql(sqlStatement.toString());
                        }

                        ops.putSqlCache(sqlCacheConfig);
                        ops.commit();

                        if (sqlStatement == null) {
                            String sql = sqlCacheConfig.getSql();
                            sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
                        }

                        return MycatdbCommand.execute(dataContext, response, sqlStatement);
                    }
                    if ("showSqlCaches".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
                        resultSetBuilder.addColumnInfo("info", JDBCType.VARCHAR);
                        if (MetaClusterCurrent.exist(SqlResultSetService.class)) {
                            SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
                            sqlResultSetService.snapshot().toStringList()
                                    .forEach(c -> resultSetBuilder.addObjectRowPayload(Arrays.asList(c)));
                        }
                        return response.sendResultSet(resultSetBuilder.build());
                    }
                    if ("dropSqlCache".equalsIgnoreCase(cmd)) {
                        MycatRouterConfigOps ops = ConfigUpdater.getOps();
                        SqlCacheConfig sqlCacheConfig = JsonUtil.from(body, SqlCacheConfig.class);
                        ops.removeSqlCache(sqlCacheConfig.getName());
                        ops.commit();
                        return response.sendOk();
                    }
                    if ("showBufferUsage".equalsIgnoreCase(cmd)) {
                        return response.sendResultSet(mycatServer.showBufferUsage(dataContext.getSessionId()));
                    }
                    if ("showUsers".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        builder.addColumnInfo("username", JDBCType.VARCHAR);
                        builder.addColumnInfo("ip", JDBCType.VARCHAR);
                        builder.addColumnInfo("transactionType", JDBCType.VARCHAR);
                        builder.addColumnInfo("dbType", JDBCType.VARCHAR);
                        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
                        List<UserConfig> userConfigs = authenticator.getConfigAsList();
                        for (UserConfig userConfig : userConfigs) {
                            builder.addObjectRowPayload(Arrays.asList(
                                    userConfig.getUsername(),
                                    userConfig.getIp(),
                                    userConfig.getTransactionType(),
                                    userConfig.getDialect()
                            ));
                        }
                        return response.sendResultSet(() -> builder.build());
                    }

                    if ("showSchemas".equalsIgnoreCase(cmd)) {
                        Map map = JsonUtil.from(body, Map.class);
                        String schemaName = (String) map.get("schemaName");
                        Collection<SchemaHandler> schemaHandlers;
                        if (schemaName == null) {
                            schemaHandlers = metadataManager.getSchemaMap().values();
                        } else {
                            schemaHandlers = Collections.singletonList(
                                    metadataManager.getSchemaMap().get(schemaName)
                            );
                        }
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        builder.addColumnInfo("SCHEMA_NAME", JDBCType.VARCHAR)
                                .addColumnInfo("DEFAULT_TARGET_NAME", JDBCType.VARCHAR)
                                .addColumnInfo("TABLE_NAMES", JDBCType.VARCHAR);
                        for (SchemaHandler value : schemaHandlers) {
                            String SCHEMA_NAME = value.getName();
                            String DEFAULT_TARGET_NAME = value.defaultTargetName();
                            String TABLE_NAMES = String.join(",", value.logicTables().keySet());
                            builder.addObjectRowPayload(Arrays.asList(SCHEMA_NAME, DEFAULT_TARGET_NAME, TABLE_NAMES));
                        }
                        return response.sendResultSet(() -> builder.build());
                    }

                    if ("showTables".equalsIgnoreCase(cmd)) {
                        return showTables(response, body, metadataManager, routerConfig);
                    }
                    if ("setSqlTimeFilter".equalsIgnoreCase(cmd)) {
                        return setSqlTimeFilter(response, body, metadataManager);
                    }
                    if ("getSqlTimeFilter".equalsIgnoreCase(cmd)) {
                        return getSqlTimeFilter(response, body, metadataManager);
                    }
                    if ("showClusters".equalsIgnoreCase(cmd)) {
                        Map map = JsonUtil.from(body, Map.class);
                        String clusterName = (String) map.get("name");
                        RowBaseIterator rowBaseIterator = showClusters(clusterName);
                        return response.sendResultSet(rowBaseIterator);
                    }
                    if ("showNativeDataSources".equalsIgnoreCase(cmd)) {
                        return response.sendResultSet(mycatServer.showNativeDataSources());
                    }
                    if ("showDataSources".equalsIgnoreCase(cmd)) {

                        Optional<JdbcConnectionManager> connectionManager = Optional.ofNullable(jdbcConnectionManager);
                        Collection<JdbcDataSource> jdbcDataSources = new HashSet<>(connectionManager.map(i -> i.getDatasourceInfo()).map(i -> i.values()).orElse(Collections.emptyList()));
                        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();

                        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("USERNAME", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("PASSWORD", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("MAX_CON", JDBCType.BIGINT);
                        resultSetBuilder.addColumnInfo("MIN_CON", JDBCType.BIGINT);
                        resultSetBuilder.addColumnInfo("EXIST_CON", JDBCType.BIGINT);
                        resultSetBuilder.addColumnInfo("USE_CON", JDBCType.BIGINT);
                        resultSetBuilder.addColumnInfo("MAX_RETRY_COUNT", JDBCType.BIGINT);
                        resultSetBuilder.addColumnInfo("MAX_CONNECT_TIMEOUT", JDBCType.BIGINT);
                        resultSetBuilder.addColumnInfo("DB_TYPE", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("URL", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.VARCHAR);

                        resultSetBuilder.addColumnInfo("INIT_SQL", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("INIT_SQL_GET_CONNECTION", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("INSTANCE_TYPE", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("IDLE_TIMEOUT", JDBCType.BIGINT);
                        resultSetBuilder.addColumnInfo("DRIVER", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("IS_MYSQL", JDBCType.VARCHAR);

                        for (JdbcDataSource jdbcDataSource : jdbcDataSources) {
                            DatasourceConfig config = jdbcDataSource.getConfig();
                            String NAME = config.getName();
                            String USERNAME = config.getUser();
                            String PASSWORD = config.getPassword();
                            int MAX_CON = config.getMaxCon();
                            int MIN_CON = config.getMinCon();

                            int USED_CON = jdbcDataSource.getUsedCount();//注意显示顺序
                            int EXIST_CON = USED_CON;//jdbc连接池已经存在连接数量是内部状态,未知
                            int MAX_RETRY_COUNT = config.getMaxRetryCount();
                            long MAX_CONNECT_TIMEOUT = config.getMaxConnectTimeout();
                            String DB_TYPE = config.getDbType();
                            String URL = config.getUrl();
                            int WEIGHT = config.getWeight();
                            String INIT_SQL = Optional.ofNullable(config.getInitSqls()).map(o -> String.join(";", o)).orElse("");
                            boolean INIT_SQL_GET_CONNECTION = config.isInitSqlsGetConnection();

                            String INSTANCE_TYPE = Optional.ofNullable(replicaSelectorRuntime.getPhysicsInstanceByName(NAME)).map(i -> i.getType().name()).orElse(config.getInstanceType());
                            long IDLE_TIMEOUT = config.getIdleTimeout();
                            String DRIVER = jdbcDataSource.getDataSource().toString();//保留属性
                            String TYPE = config.getType();
                            boolean IS_MYSQL = jdbcDataSource.isMySQLType();

                            resultSetBuilder.addObjectRowPayload(Arrays.asList(NAME, USERNAME, PASSWORD, MAX_CON, MIN_CON, EXIST_CON, USED_CON,
                                    MAX_RETRY_COUNT, MAX_CONNECT_TIMEOUT, DB_TYPE, URL, WEIGHT, INIT_SQL, INIT_SQL_GET_CONNECTION, INSTANCE_TYPE,
                                    IDLE_TIMEOUT, DRIVER, TYPE, IS_MYSQL));
                        }

                        return response.sendResultSet(() -> resultSetBuilder.build());
                    }
                    if ("showHeartbeats".equalsIgnoreCase(cmd)) {
                        RowBaseIterator rowBaseIterator = showHeatbeatStat();
                        return response.sendResultSet(rowBaseIterator);
                    }
                    if ("showHeartbeatStatus".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        builder.addColumnInfo("name", JDBCType.VARCHAR);
                        builder.addColumnInfo("status", JDBCType.VARCHAR);
                        Map<String, HeartbeatFlow> heartbeatDetectorMap = replicaSelectorRuntime.getHeartbeatDetectorMap();
                        for (Map.Entry<String, HeartbeatFlow> entry : heartbeatDetectorMap.entrySet()) {
                            String key = entry.getKey();
                            HeartbeatFlow value = entry.getValue();
                            builder.addObjectRowPayload(Arrays.asList(
                                    Objects.toString(key),
                                    Objects.toString(value.getDsStatus())
                            ));
                        }
                        return response.sendResultSet(() -> builder.build());
                    }
                    if ("showInstances".equalsIgnoreCase(cmd)) {
                        RowBaseIterator rowBaseIterator = showInstances();
                        return response.sendResultSet(rowBaseIterator);
                    }
                    if ("showReactors".equalsIgnoreCase(cmd)) {
                        MycatServer server = MetaClusterCurrent.wrapper(MycatServer.class);
                        return response.sendResultSet(server.showReactors());
                    }
                    if ("showThreadPools".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        builder.addColumnInfo("NAME", JDBCType.VARCHAR)
                                .addColumnInfo("POOL_SIZE", JDBCType.BIGINT)
                                .addColumnInfo("ACTIVE_COUNT", JDBCType.BIGINT)
                                .addColumnInfo("TASK_QUEUE_SIZE", JDBCType.BIGINT)
                                .addColumnInfo("COMPLETED_TASK", JDBCType.BIGINT)
                                .addColumnInfo("TOTAL_TASK", JDBCType.BIGINT);

                        return response.sendResultSet(() -> builder.build());
                    }
                    if ("showNativeBackends".equalsIgnoreCase(cmd)) {
                        MycatServer server = MetaClusterCurrent.wrapper(MycatServer.class);
                        return response.sendResultSet(server.showNativeBackends());
                    }
                    if ("showConnections".equalsIgnoreCase(cmd)) {
                        MycatServer server = MetaClusterCurrent.wrapper(MycatServer.class);
                        return response.sendResultSet(server.showConnections());
                    }
                    if ("showSchedules".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        ScheduledExecutorService timer = ScheduleUtil.getTimer();
                        String NAME = timer.toString();
                        boolean IS_TERMINATED = timer.isTerminated();
                        boolean IS_SHUTDOWN = timer.isShutdown();
                        int SCHEDULE_COUNT = ScheduleUtil.getScheduleCount();
                        builder.addColumnInfo("NAME", JDBCType.VARCHAR)
                                .addColumnInfo("IS_TERMINATED", JDBCType.VARCHAR)
                                .addColumnInfo("IS_SHUTDOWN", JDBCType.VARCHAR)
                                .addColumnInfo("SCHEDULE_COUNT", JDBCType.BIGINT);
                        builder.addObjectRowPayload(Arrays.asList(NAME, IS_TERMINATED, IS_SHUTDOWN, SCHEDULE_COUNT));
                        return response.sendResultSet(() -> builder.build());
                    }
                    if ("showBaselines".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        QueryPlanCache queryPlanCache = MetaClusterCurrent.wrapper(QueryPlanCache.class);
                        builder.addColumnInfo("BASELINE_ID", JDBCType.VARCHAR)
                                .addColumnInfo("PARAMETERIZED_SQL", JDBCType.VARCHAR)
                                .addColumnInfo("PLAN_ID", JDBCType.VARCHAR)
                                .addColumnInfo("EXTERNALIZED_PLAN", JDBCType.VARCHAR)
                                .addColumnInfo("FIXED", JDBCType.VARCHAR)
                                .addColumnInfo("ACCEPTED", JDBCType.VARCHAR);
                        for (Baseline baseline : queryPlanCache.list()) {
                            for (BaselinePlan baselinePlan : baseline.getPlanList()) {
                                String BASELINE_ID = String.valueOf(baselinePlan.getBaselineId());
                                String PARAMETERIZED_SQL = String.valueOf(baselinePlan.getSql());
                                String PLAN_ID = String.valueOf(baselinePlan.getId());
                                CodeExecuterContext attach = (CodeExecuterContext) baselinePlan.attach();
                                String EXTERNALIZED_PLAN = new PlanImpl(attach.getMycatRel(), attach, Collections.emptyList()).dumpPlan();
                                String FIXED = Optional.ofNullable(baseline.getFixPlan()).filter(i -> i.getId() == baselinePlan.getId())
                                        .map(u -> "true").orElse("false");
                                String ACCEPTED = "true";

                                builder.addObjectRowPayload(Arrays.asList(BASELINE_ID, PARAMETERIZED_SQL, PLAN_ID, EXTERNALIZED_PLAN, FIXED, ACCEPTED));
                            }
                        }
                        return response.sendResultSet(() -> builder.build());
                    }
                    if ("showConfigText".equalsIgnoreCase(cmd)) {
                        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
                        String text = JsonUtil.toJson(mycatRouterConfig);
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        builder.addColumnInfo("CONFIG_TEXT", JDBCType.VARCHAR);
                        builder.addObjectRowPayload(Arrays.asList(text));
                        return response.sendResultSet(builder.build());
                    }
                    if ("setBkaJoin".equalsIgnoreCase(cmd)) {
                        DrdsSqlCompiler.RBO_BKA_JOIN = body.contains("1");
                        return response.sendOk();
                    }
                    if ("setSortMergeJoin".equalsIgnoreCase(cmd)) {
                        DrdsSqlCompiler.RBO_MERGE_JOIN = body.contains("1");
                        return response.sendOk();
                    }
                    if ("setAcceptConnect".equalsIgnoreCase(cmd)) {
                        boolean contains = body.contains("1");
                        MycatServer server = MetaClusterCurrent.wrapper(MycatServer.class);
                        if (!contains) {
                            server.stopAcceptConnect();
                        } else {
                            server.resumeAcceptConnect();
                        }
                        dataContext.setAffectedRows(1);
                        return response.sendOk();
                    }
                    if ("setReadyToCloseSQL".equalsIgnoreCase(cmd)) {
                        ReadyToCloseSQLHint readyToCloseSQLHint = JsonUtil.from(body, ReadyToCloseSQLHint.class);
                        String sql = readyToCloseSQLHint.getSql().trim();
                        MycatServer server = MetaClusterCurrent.wrapper(MycatServer.class);
                        server.setReadyToCloseSQL(sql);
                        dataContext.setAffectedRows(1);
                        return response.sendOk();
                    }
                    if ("pauseServer".equalsIgnoreCase(cmd)) {
                        PauseServerHint pauseServerHint = JsonUtil.from(body, PauseServerHint.class);
                        MycatServer server = MetaClusterCurrent.wrapper(MycatServer.class);

                        long sessionId = dataContext.getSessionId();
                        HashSet<Long> set = new HashSet<>();
                        set.add(sessionId);
                        set.addAll(Optional.ofNullable(pauseServerHint.getConnectionIds()).orElse(Collections.emptyList()));
                        return server.pause(new ArrayList<>(set)).transform(result -> {
                            if (result.succeeded()) {
                                dataContext.setAffectedRows(1);
                                return response.sendOk();
                            } else {
                                server.resume();
                                return response.sendError(result.cause());
                            }
                        });
                    }
                    if ("resumeServer".equalsIgnoreCase(cmd)) {
                        MycatServer server = MetaClusterCurrent.wrapper(MycatServer.class);
                        server.resume();
                        dataContext.setAffectedRows(1);
                        return response.sendOk();
                    }
                    if ("setDebug".equalsIgnoreCase(cmd)) {
                        boolean contains = body.contains("1");
                        dataContext.setDebug(contains);
                        return response.sendOk();
                    }
                    if ("setVector".equalsIgnoreCase(cmd)) {
                        boolean contains = body.contains("1");
                        dataContext.setVector(contains);
                        return response.sendOk();
                    }
                    if ("is".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder builder = ResultSetBuilder.create();
                        builder.addColumnInfo("value", JDBCType.VARCHAR);
                        if (body.contains("debug")) {
                            boolean debug = dataContext.isDebug();
                            DrdsSqlCompiler.DEBUG = debug;
                            builder.addObjectRowPayload(Arrays.asList(debug ? "1" : "0"));
                        }
                        return response.sendResultSet(builder.build());
                    }
                    if ("setBkaJoinLeftRowCountLimit".equalsIgnoreCase(cmd)) {
                        DrdsSqlCompiler.BKA_JOIN_LEFT_ROW_COUNT_LIMIT = Long.parseLong(
                                body.substring(1, body.length() - 1));
                        return response.sendOk();
                    }
                    if ("baseline".equalsIgnoreCase(cmd)) {
                        Map<String, Object> map = JsonUtil.from(body, Map.class);
                        String command = Objects.requireNonNull(map.get("command")).toString().toLowerCase();
                        long value = Long.parseLong((map.getOrDefault("value", "0")).toString());
                        QueryPlanCache queryPlanCache = MetaClusterCurrent.wrapper(QueryPlanCache.class);
                        switch (command) {
                            case "showAllPlans": {
                                ResultSetBuilder builder = ResultSetBuilder.create();

                                builder.addColumnInfo("BASELINE_ID", JDBCType.VARCHAR)
                                        .addColumnInfo("PARAMETERIZED_SQL", JDBCType.VARCHAR)
                                        .addColumnInfo("PLAN_ID", JDBCType.VARCHAR)
                                        .addColumnInfo("EXTERNALIZED_PLAN", JDBCType.VARCHAR)
                                        .addColumnInfo("FIXED", JDBCType.VARCHAR)
                                        .addColumnInfo("ACCEPTED", JDBCType.VARCHAR);
                                for (Baseline baseline : queryPlanCache.list()) {
                                    for (BaselinePlan baselinePlan : baseline.getPlanList()) {
                                        String BASELINE_ID = String.valueOf(baselinePlan.getBaselineId());
                                        String PARAMETERIZED_SQL = String.valueOf(baselinePlan.getSql());
                                        String PLAN_ID = String.valueOf(baselinePlan.getId());
                                        CodeExecuterContext attach = (CodeExecuterContext) baselinePlan.attach();
                                        String EXTERNALIZED_PLAN = new PlanImpl(attach.getMycatRel(), attach, Collections.emptyList()).dumpPlan();
                                        String FIXED = Optional.ofNullable(baseline.getFixPlan()).filter(i -> i.getId() == baselinePlan.getId())
                                                .map(u -> "true").orElse("false");
                                        String ACCEPTED = "true";

                                        builder.addObjectRowPayload(Arrays.asList(BASELINE_ID, PARAMETERIZED_SQL, PLAN_ID, EXTERNALIZED_PLAN, FIXED, ACCEPTED));
                                    }
                                }
                                return response.sendResultSet(() -> builder.build());
                            }
                            case "persistAllBaselines": {
                                queryPlanCache.saveBaselines();
                                return response.sendOk();
                            }
                            case "loadBaseline": {
                                queryPlanCache.loadBaseline(value);
                                return response.sendOk();
                            }
                            case "loadPlan": {
                                queryPlanCache.loadPlan(value);
                                return response.sendOk();
                            }
                            case "persistPlan": {
                                queryPlanCache.persistPlan(value);
                                return response.sendOk();
                            }
                            case "clearBaseline": {
                                queryPlanCache.clearBaseline(value);
                                return response.sendOk();
                            }
                            case "clearPlan": {
                                queryPlanCache.clearPlan(value);
                                return response.sendOk();
                            }
                            case "deleteBaseline": {
                                queryPlanCache.deleteBaseline(value);
                                return response.sendOk();
                            }
                            case "deletePlan": {
                                queryPlanCache.deletePlan(value);
                                return response.sendOk();
                            }
                            case "add":
                            case "fix": {
                                SQLStatement sqlStatement = null;
                                if (ast.getHintStatements() != null && ast.getHintStatements().size() == 1) {
                                    sqlStatement = ast.getHintStatements().get(0);
                                    DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlStatement, dataContext.getDefaultSchema());
                                    queryPlanCache.add("fix".equalsIgnoreCase(command), drdsSqlWithParams);
                                }
                                return response.sendOk();
                            }
                            default:
                                throw new UnsupportedOperationException();
                        }
                    }
                    if ("MIGRATE_LIST".equalsIgnoreCase(cmd)) {
                        return response.sendResultSet(MigrateUtil.list());
                    }
                    if ("MIGRATE_STOP".equalsIgnoreCase(cmd)) {
                        MigrateStopHint hint = JsonUtil.from(body, MigrateStopHint.class);
                        if (MigrateUtil.stop(hint.getId())) {
                            dataContext.setAffectedRows(1);
                        }
                        return response.sendOk();
                    }
                    if ("MIGRATE".equalsIgnoreCase(cmd)) {
                        MigrateHint migrateHint = JsonUtil.from(body, MigrateHint.class);
                        String name = migrateHint.getName();
                        MigrateHint.Input input = migrateHint.getInput();
                        MigrateHint.Output output = migrateHint.getOutput();
                        int parallelism = output.getParallelism();

                        MigrateUtil.MigrateJdbcOutput migrateJdbcOutput = new MigrateUtil.MigrateJdbcOutput();
                        migrateJdbcOutput.setParallelism(parallelism);
                        migrateJdbcOutput.setBatch(output.getBatch());

                        List<MigrateUtil.MigrateJdbcInput> migrateJdbcInputs = new ArrayList<>();
                        List<Flowable<Object[]>> observables = new ArrayList<>();
                        MetadataManager manager = MetaClusterCurrent.wrapper(MetadataManager.class);
                        TableHandler outputTable = manager.getTable(output.getSchemaName(), output.getTableName());
                        String username = Optional.ofNullable(output.getUsername()).orElseGet(new Supplier<String>() {
                            @Override
                            public String get() {
                                UserConfig userConfig = routerConfig.getUsers().get(0);
                                String username = userConfig.getUsername();
                                String password = userConfig.getPassword();
                                return username;
                            }
                        });
                        String password = Optional.ofNullable(output.getUsername()).orElseGet(new Supplier<String>() {
                            @Override
                            public String get() {

                                UserConfig userConfig = routerConfig.getUsers().get(0);
                                String username = userConfig.getUsername();
                                String password = userConfig.getPassword();
                                return password;
                            }
                        });
                        String url = Optional.ofNullable(output.getUrl()).orElseGet(() -> {
                            ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
                            String ip = serverConfig.getIp();
                            int port = serverConfig.getPort();
                            return "jdbc:mysql://" +
                                    ip +
                                    ":" +
                                    port + "/mysql?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";
                        });

                        if (input.getUrl() != null) {
                            String sql = input.getSql();
                            long count = input.getCount();
                            Map<String, String> properties = input.getProperties();

                            MigrateUtil.MigrateJdbcInput migrateJdbcInput = new MigrateUtil.MigrateJdbcInput();
                            migrateJdbcInput.setCount(count);
                            observables.add(MigrateUtil.read(migrateJdbcInput, input.getUrl(), input.getUsername(), input.getPassword(), sql));
                        } else if (input.getType() == null || "mycat".equalsIgnoreCase(input.getType())) {

                            TableHandler inputTable = manager.getTable(input.getSchemaName(), input.getTableName());


                            switch (inputTable.getType()) {
                                case SHARDING: {
                                    ShardingTable shardingTable = (ShardingTable) inputTable;
                                    for (Partition backend : shardingTable.getBackends()) {
                                        MigrateUtil.MigrateJdbcInput migrateJdbcInput = new MigrateUtil.MigrateJdbcInput();
                                        migrateJdbcInputs.add(migrateJdbcInput);
                                        observables.add(MigrateUtil.read(migrateJdbcInput, backend));
                                    }
                                    break;
                                }
                                case GLOBAL: {
                                    GlobalTable globalTable = (GlobalTable) inputTable;
                                    Partition partition = globalTable.getGlobalDataNode().get(0);
                                    MigrateUtil.MigrateJdbcInput migrateJdbcInput = new MigrateUtil.MigrateJdbcInput();
                                    migrateJdbcInputs.add(migrateJdbcInput);
                                    observables.add(MigrateUtil.read(migrateJdbcInput, partition));
                                    break;
                                }
                                case NORMAL: {
                                    NormalTable normalTable = (NormalTable) inputTable;
                                    Partition partition = normalTable.getDataNode();
                                    MigrateUtil.MigrateJdbcInput migrateJdbcInput = new MigrateUtil.MigrateJdbcInput();
                                    migrateJdbcInputs.add(migrateJdbcInput);
                                    observables.add(MigrateUtil.read(migrateJdbcInput, partition));
                                    break;
                                }
                                case VISUAL:
                                case VIEW:
                                case CUSTOM:
                                    MigrateUtil.MigrateJdbcInput migrateJdbcInput = new MigrateUtil.MigrateJdbcInput();
                                    migrateJdbcInputs.add(migrateJdbcInput);
                                    observables.add(MigrateUtil.read(migrateJdbcInput, input.getTableName(), input.getSchemaName(), url, username, password));
                                    break;
                                default:
                                    throw new IllegalStateException("Unexpected value: " + inputTable.getType());
                            }

                        } else {
                            throw new UnsupportedOperationException();
                        }

                        String outputSchemaName = outputTable.getSchemaName();
                        String outputTableName = outputTable.getTableName();


                        String insertTemplate = getMySQLInsertTemplate(outputTable);


                        migrateJdbcOutput.setUsername(username);
                        migrateJdbcOutput.setPassword(password);
                        migrateJdbcOutput.setUrl(url);
                        migrateJdbcOutput.setInsertTemplate(insertTemplate);

                        MigrateUtil.MigrateController migrateController = MigrateUtil
                                .write(migrateJdbcOutput, Flowable.merge(observables.stream().map(i -> i.buffer(output.getBatch()).subscribeOn(Schedulers.io())).collect(Collectors.toList())));
                        MigrateUtil.MigrateScheduler scheduler = MigrateUtil.register(name, migrateJdbcInputs, migrateJdbcOutput, migrateController);
                        return response.sendResultSet(MigrateUtil.show(scheduler));
                    }
                    if ("BINLOG_LIST".equalsIgnoreCase(cmd)) {
                        return response.sendResultSet(BinlogUtil.list());
                    }
                    if ("BINLOG_STOP".equalsIgnoreCase(cmd)) {
                        BinlogStopHint hint = JsonUtil.from(body, BinlogStopHint.class);
                        if (BinlogUtil.stop(hint.getId())) {
                            dataContext.setAffectedRows(1);
                        }
                        return response.sendOk();
                    }
                    if ("BINLOG_CLEAR".equalsIgnoreCase(cmd)) {
                        BinlogUtil.clear();
                        dataContext.setAffectedRows(1);
                        return response.sendOk();
                    }
                    if ("BINLOG_SNAPSHOT".equalsIgnoreCase(cmd)) {
                        BinlogSnapshotHint hint = JsonUtil.from(body, BinlogSnapshotHint.class);
                        RowBaseIterator rowBaseIterator = BinlogUtil.binlogSnapshot(hint.getName());
                        return response.sendResultSet(rowBaseIterator);
                    }
                    if ("BINLOG_SYNC".equalsIgnoreCase(cmd)) {
                        BinlogSyncHint binlogHint = JsonUtil.from(body, BinlogSyncHint.class);
                        String name = binlogHint.getName();
                        String snapshot = binlogHint.getSnapshotId();
                        Map<String, BinlogUtil.BinlogArgs> binlogArgsMap = new HashMap<>();
                        List<BinlogUtil.BinlogRes> binlogResArrayList = new ArrayList<>();
                        if (snapshot != null) {
                            try (DefaultConnection defaultConnection = jdbcConnectionManager.getConnection(MetadataManager.getPrototype());) {
                                List<Map<String, Object>> maps = JdbcUtils.executeQuery(defaultConnection.getRawConnection(), "select * from mycat.ds_binlog where Id = ?", Arrays.asList(snapshot));
                                for (Map<String, Object> map : maps) {
                                    String eId = (String) map.get("Id");
                                    String eName = (String) map.get("Name");
                                    String datasource = (String) map.get("Datasource");
                                    String file = (String) map.get("File");
                                    Long position = (Long) map.get("Position");
                                    String binlog_ignore_db = (String) map.get("Binlog_Ignore_DB");
                                    String binlog_do_db = (String) map.get("Binlog_Do_DB");

                                    BinlogUtil.BinlogArgs binlogArgs = new BinlogUtil.BinlogArgs();
                                    binlogArgs.setBinlogFilename(file);
                                    binlogArgs.setBinlogPosition(position);

                                    if (binlogHint.getConnectTimeout() > 0) {
                                        binlogArgs.setConnectTimeout(binlogHint.getConnectTimeout());
                                    }
                                    binlogArgsMap.put(datasource, binlogArgs);
                                }
                            }
                        }
                        Objects.requireNonNull(binlogHint.getInputTableNames());

                        List<String> outputTableNames = binlogHint.getOutputTableNames();

                        if (outputTableNames == null) {
                            binlogHint.setOutputTableNames(binlogHint.getInputTableNames());
                        }

                        IdentityHashMap<TableHandler, TableHandler> map = new IdentityHashMap<>();


                        List<TableHandler> inputs = new ArrayList<>();
                        List<TableHandler> outputs = new ArrayList<>();

                        for (String inputTableName : binlogHint.getInputTableNames()) {
                            String[] split = inputTableName.split("\\.");
                            String schemaName = SQLUtils.normalize(split[0]);
                            String tableName = SQLUtils.normalize(split[1]);
                            TableHandler inputTable = metadataManager.getTable(schemaName, tableName);
                            inputs.add(inputTable);
                        }

                        for (String outputTableName : binlogHint.getOutputTableNames()) {
                            String[] split = outputTableName.split("\\.");
                            String schemaName = SQLUtils.normalize(split[0]);
                            String tableName = SQLUtils.normalize(split[1]);
                            TableHandler outputTable = metadataManager.getTable(schemaName, tableName);
                            outputs.add(outputTable);
                        }

                        for (int i = 0; i < inputs.size(); i++) {
                            map.put(inputs.get(i), outputs.get(i));
                        }

                        Map<String, Map<String, List<Partition>>> infoCollector = new HashMap<>();
                        List<MigrateUtil.MigrateController> migrateControllers = new ArrayList<>();


                        Set<Map.Entry<TableHandler, TableHandler>> entries = map.entrySet();
                        for (Map.Entry<TableHandler, TableHandler> entry : entries) {
                            ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);

                            TableHandler inputTable = entry.getKey();
                            TableHandler outputTable = entry.getValue();

                            UserConfig userConfig = routerConfig.getUsers().get(0);

                            String username = userConfig.getUsername();
                            String password = userConfig.getPassword();


                            String ip = serverConfig.getIp();
                            int port = serverConfig.getPort();

                            String url =
                                    "jdbc:mysql://" +
                                            ip +
                                            ":" +
                                            port + "/mysql?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";

                            MigrateUtil.MigrateJdbcAnyOutput output = new MigrateUtil.MigrateJdbcAnyOutput();
                            output.setUrl(url);
                            output.setUsername(username);
                            output.setPassword(password);

                            List<Partition> partitions = new ArrayList<>();
                            switch (inputTable.getType()) {
                                case SHARDING: {
                                    ShardingTable shardingTable = (ShardingTable) inputTable;
                                    partitions = shardingTable.getShardingFuntion().calculate(Collections.emptyMap());
                                    break;
                                }
                                case GLOBAL: {
                                    GlobalTable globalTable = (GlobalTable) inputTable;
                                    partitions = ImmutableList.of(globalTable.getGlobalDataNode().get(0));
                                    break;
                                }
                                case NORMAL: {
                                    NormalTable normalTable = (NormalTable) inputTable;
                                    partitions = ImmutableList.of(normalTable.getDataNode());
                                    break;
                                }
                                case CUSTOM:
                                case VISUAL:
                                case VIEW:
                                    throw new UnsupportedOperationException();
                            }
                            ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);

                            Map<String, List<Partition>> listMap = partitions.stream().collect(Collectors.groupingBy(partition -> replicaSelectorManager.getDatasourceNameByReplicaName(partition.getTargetName(), true, null)));
                            infoCollector.put(inputTable.getUniqueName(), listMap);

                            List<Flowable<BinlogUtil.ParamSQL>> flowables = new ArrayList<>();

                            for (Map.Entry<String, List<Partition>> e : listMap.entrySet()) {
                                BinlogUtil.BinlogArgs binlogArgs = binlogArgsMap.get(e.getKey());
                                BinlogUtil.BinlogRes binlogRes = new BinlogUtil.BinlogRes();
                                binlogResArrayList.add(binlogRes);
                                flowables.add(BinlogUtil.observe(binlogArgs, binlogRes, e.getKey(), e.getValue()).subscribeOn(Schedulers.io()));
                            }
                            Flowable<BinlogUtil.ParamSQL> merge = flowables.size() == 1 ? flowables.get(0) : Flowable.merge(flowables, flowables.size());
                            merge = merge.map(paramSQL -> {
                                SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(paramSQL.getSql());
                                SQLExprTableSource sqlExprTableSource = VertxUpdateExecuter.getTableSource(sqlStatement);
                                MycatSQLExprTableSourceUtil.setSqlExprTableSource(outputTable.getSchemaName(), outputTable.getTableName(), sqlExprTableSource);
                                paramSQL.setSql(sqlStatement.toString());
                                return paramSQL;
                            });
                            MigrateUtil.MigrateController migrateController = MigrateUtil.writeSql(output, merge);
                            migrateControllers.add(migrateController);
                        }
                        BinlogUtil.BinlogScheduler scheduler = BinlogUtil.BinlogScheduler.of(UUID.randomUUID().toString(), binlogHint.getName(), infoCollector, migrateControllers,binlogResArrayList);
                        BinlogUtil.register(scheduler);
                        return response.sendResultSet(BinlogUtil.list(Collections.singletonList(scheduler)));
                    }
                    mycatDmlHandler(cmd, body);
                    return response.sendOk();
                }
            }
            return response.sendOk();
        } catch (Throwable throwable) {
            return response.sendError(throwable);
        }
    }

    private static String getMySQLInsertTemplate(TableHandler outputTable) {
        String outputSchemaName = outputTable.getSchemaName();
        String outputTableName = outputTable.getTableName();
        MySqlInsertStatement mySqlInsertStatement = new MySqlInsertStatement();
        mySqlInsertStatement.setTableName(new SQLIdentifierExpr("`" + outputTableName + "`"));
        mySqlInsertStatement.getTableSource().setSchema("`" + outputSchemaName + "`");
        SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();

        int columnCount = outputTable.getColumns().size();

        for (SimpleColumnInfo column : outputTable.getColumns()) {
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`" + column.getColumnName() + "`"));
            valuesClause.addValue(new SQLVariantRefExpr("?"));
        }
        mySqlInsertStatement.setValues(valuesClause);
        return mySqlInsertStatement.toString();
    }

    public static RowBaseIterator showHeatbeatStat() {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("READABLE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("SESSION_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("ALIVE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("MASTER", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("LIMIT_SESSION_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("REPLICA", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("SLAVE_THRESHOLD", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("IS_HEARTBEAT_TIMEOUT", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("HB_ERROR_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("HB_LAST_SWITCH_TIME", JDBCType.TIMESTAMP);
        resultSetBuilder.addColumnInfo("HB_MAX_RETRY", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("IS_CHECKING", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("MIN_SWITCH_TIME_INTERVAL", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("HEARTBEAT_TIMEOUT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("SYNC_DS_STATUS", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("HB_DS_STATUS", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("IS_SLAVE_BEHIND_MASTER", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("LAST_SEND_QUERY_TIME", JDBCType.TIMESTAMP);
        resultSetBuilder.addColumnInfo("LAST_RECEIVED_QUERY_TIME", JDBCType.TIMESTAMP);


        if (MetaClusterCurrent.exist(MycatRouterConfig.class) && MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
            MycatRouterConfig routerConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
            ReplicaSelectorManager replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            Map<String, DatasourceConfig> dataSourceConfig = routerConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));

            for (HeartbeatFlow heartbeatFlow : replicaSelectorRuntime.getHeartbeatDetectorMap().values()) {
                PhysicsInstance instance = heartbeatFlow.instance();

                String NAME = instance.getName();
                String TYPE = instance.getType().name();
                boolean READABLE = instance.asSelectRead();
                int SESSION_COUNT = instance.getSessionCounter();
                int WEIGHT = instance.getWeight();
                boolean ALIVE = instance.isAlive();
                boolean MASTER = instance.isMaster();

                double SLAVE_THRESHOLD = heartbeatFlow.getSlaveThreshold();

                boolean IS_HEARTBEAT_TIMEOUT = heartbeatFlow.isHeartbeatTimeout();
                final HeartBeatStatus HEART_BEAT_STATUS = heartbeatFlow.getHbStatus();
                int HB_ERROR_COUNT = HEART_BEAT_STATUS.getErrorCount();
                LocalDateTime HB_LAST_SWITCH_TIME =
                        new Timestamp(HEART_BEAT_STATUS.getLastSwitchTime()).toLocalDateTime();
                int HB_MAX_RETRY = HEART_BEAT_STATUS.getMaxRetry();
                boolean IS_CHECKING = HEART_BEAT_STATUS.isChecking();
                long MIN_SWITCH_TIME_INTERVAL = HEART_BEAT_STATUS.getMinSwitchTimeInterval();
                final long HEARTBEAT_TIMEOUT = (heartbeatFlow.getHeartbeatTimeout());
                DatasourceStatus DS_STATUS_OBJECT = heartbeatFlow.getDsStatus();
                String SYNC_DS_STATUS = DS_STATUS_OBJECT.getDbSynStatus().name();
                String HB_DS_STATUS = DS_STATUS_OBJECT.getStatus().name();
                boolean IS_SLAVE_BEHIND_MASTER = DS_STATUS_OBJECT.isSlaveBehindMaster();
                LocalDateTime LAST_SEND_QUERY_TIME =
                        new Timestamp(heartbeatFlow.getLastSendQryTime()).toLocalDateTime();
                LocalDateTime LAST_RECEIVED_QUERY_TIME =
                        new Timestamp(heartbeatFlow.getLastReceivedQryTime()).toLocalDateTime();
                Optional<DatasourceConfig> e = Optional.ofNullable(dataSourceConfig.get(NAME));

                String replicaDataSourceSelectorList = String.join(",", replicaSelectorRuntime.getReplicaNameListByInstanceName(NAME));

                resultSetBuilder.addObjectRowPayload(
                        Arrays.asList(NAME,
                                TYPE,
                                READABLE,
                                SESSION_COUNT,
                                WEIGHT,
                                ALIVE,
                                MASTER,
                                e.map(i -> i.getMaxCon()).orElse(-1),
                                replicaDataSourceSelectorList,
                                SLAVE_THRESHOLD,
                                IS_HEARTBEAT_TIMEOUT,
                                HB_ERROR_COUNT,
                                HB_LAST_SWITCH_TIME,
                                HB_MAX_RETRY,
                                IS_CHECKING,
                                MIN_SWITCH_TIME_INTERVAL,
                                HEARTBEAT_TIMEOUT,
                                SYNC_DS_STATUS,
                                HB_DS_STATUS,
                                IS_SLAVE_BEHIND_MASTER,
                                LAST_SEND_QUERY_TIME,
                                LAST_RECEIVED_QUERY_TIME
                        ));
            }
        }
        RowBaseIterator rowBaseIterator = resultSetBuilder.build();
        return rowBaseIterator;
    }

    public static RowBaseIterator showInstances() {
        MycatRouterConfig routerConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        ReplicaSelectorManager replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);

        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);

        resultSetBuilder.addColumnInfo("ALIVE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("READABLE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("SESSION_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.BIGINT);

        resultSetBuilder.addColumnInfo("MASTER", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("LIMIT_SESSION_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("REPLICA", JDBCType.VARCHAR);
        Collection<PhysicsInstance> values =
                replicaSelectorRuntime.getPhysicsInstances();
        Map<String, DatasourceConfig> dataSourceConfig = routerConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));


        for (PhysicsInstance instance : values) {

            String NAME = instance.getName();
            String TYPE = instance.getType().name();
            boolean READABLE = instance.asSelectRead();
            int SESSION_COUNT = instance.getSessionCounter();
            int WEIGHT = instance.getWeight();
            boolean ALIVE = instance.isAlive();
            boolean MASTER = instance.isMaster();

            Optional<DatasourceConfig> e = Optional.ofNullable(dataSourceConfig.get(NAME));


            String replicaDataSourceSelectorList = String.join(",", replicaSelectorRuntime.getReplicaNameListByInstanceName(NAME));

            resultSetBuilder.addObjectRowPayload(
                    Arrays.asList(NAME, ALIVE, READABLE, TYPE, SESSION_COUNT, WEIGHT, MASTER,
                            e.map(i -> i.getMaxCon()).orElse(-1),
                            replicaDataSourceSelectorList
                    ));
        }
        RowBaseIterator rowBaseIterator = resultSetBuilder.build();
        return rowBaseIterator;
    }

    @Nullable
    private Future<Void> showTables(Response response, String body, MetadataManager metadataManager, MycatRouterConfig routerConfig) {
        Map map = JsonUtil.from(body, Map.class);
        String type = (String) map.get("type");
        String schemaName = (String) map.get("schemaName");

        Stream<TableHandler> tables;
        Stream<TableHandler> tableHandlerStream;
        if (schemaName == null) {
            tableHandlerStream = metadataManager.getSchemaMap().values().stream().flatMap(i -> i.logicTables().values().stream());
        } else {
            SchemaHandler schemaHandler = Objects.requireNonNull(
                    metadataManager.getSchemaMap().get(schemaName)
            );
            NameMap<TableHandler> logicTables = schemaHandler.logicTables();
            tableHandlerStream = logicTables.values().stream();
        }
        if ("global".equalsIgnoreCase(type)) {
            tables = tableHandlerStream.filter(i -> i.getType() == LogicTableType.GLOBAL);
        } else if ("sharding".equalsIgnoreCase(type)) {
            tables = tableHandlerStream.filter(i -> i.getType() == LogicTableType.SHARDING);
        } else if ("normal".equalsIgnoreCase(type)) {
            tables = tableHandlerStream.filter(i -> i.getType() == LogicTableType.NORMAL);
        } else if ("custom".equalsIgnoreCase(type)) {
            tables = tableHandlerStream.filter(i -> i.getType() == LogicTableType.CUSTOM);
        } else {
            tables = tableHandlerStream;
        }

        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("SCHEMA_NAME", JDBCType.VARCHAR)
                .addColumnInfo("TABLE_NAME", JDBCType.VARCHAR)
                .addColumnInfo("CREATE_TABLE_SQL", JDBCType.VARCHAR)
                .addColumnInfo("TYPE", JDBCType.VARCHAR)
                .addColumnInfo("COLUMNS", JDBCType.VARCHAR)
                .addColumnInfo("CONFIG", JDBCType.VARCHAR);
        tables.forEach(table -> {
            String SCHEMA_NAME = table.getSchemaName();
            String TABLE_NAME = table.getTableName();
            String CREATE_TABLE_SQL = table.getCreateTableSQL();
            LogicTableType TYPE = table.getType();
            String COLUMNS = table.getColumns().stream().map(i -> i.toString()).collect(Collectors.joining(","));
            String CONFIG = routerConfig.getSchemas().stream()
                    .filter(i -> SCHEMA_NAME.equalsIgnoreCase(i.getSchemaName()))
                    .map(i -> {
                        switch (TYPE) {
                            case SHARDING:
                                return NameMap.immutableCopyOf(i.getShardingTables());
                            case GLOBAL:
                                return NameMap.immutableCopyOf(i.getGlobalTables());
                            case NORMAL:
                                return NameMap.immutableCopyOf(i.getNormalTables());
                            case CUSTOM:
                                return NameMap.immutableCopyOf(i.getCustomTables());
                            default:
                                return null;
                        }
                    }).map(i -> i.get(TABLE_NAME)).findFirst()
                    .map(i -> i.toString()).orElse(null);
            builder.addObjectRowPayload(Arrays.asList(SCHEMA_NAME, TABLE_NAME, CREATE_TABLE_SQL, TYPE, COLUMNS, CONFIG));
        });
        return response.sendResultSet(() -> builder.build());
    }

    private Future<Void> setSqlTimeFilter(Response response, String body, MetadataManager metadataManager) {
        Map map = JsonUtil.from(body, Map.class);
        Object value = map.get("value");
        if (value != null) {
            long s = Long.parseLong(value.toString());
            if (MetaClusterCurrent.exist(MycatSQLLogMonitor.class)) {
                MycatSQLLogMonitor mycatSQLLogMonitor = MetaClusterCurrent.wrapper(MycatSQLLogMonitor.class);
                mycatSQLLogMonitor.setSqlTimeFilter(s);
            }
        }
        return response.sendOk();
    }

    private Future<Void> getSqlTimeFilter(Response response, String body, MetadataManager metadataManager) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("value", JDBCType.VARCHAR);
        long sqlTimeFilter = -1;
        if (MetaClusterCurrent.exist(MycatSQLLogMonitor.class)) {
            MycatSQLLogMonitor mycatSQLLogMonitor = MetaClusterCurrent.wrapper(MycatSQLLogMonitor.class);
            sqlTimeFilter = mycatSQLLogMonitor.getSqlTimeFilter();
        }
        resultSetBuilder.addObjectRowPayload(Arrays.asList(sqlTimeFilter));
        return response.sendResultSet(resultSetBuilder.build());
    }

    private Future<Void> showTopology(Response response, String body, MetadataManager metadataManager) {
        Map map = JsonUtil.from(body, Map.class);
        TableHandler table = metadataManager.getTable((String) map.get("schemaName"),
                (String) map.get("tableName"));
        LogicTableType type = table.getType();
        List<Partition> backends = null;
        switch (type) {
            case SHARDING:
                backends = ((ShardingTable) table).getBackends();
                break;
            case GLOBAL:
                backends = ((GlobalTable) table).getGlobalDataNode();
                break;
            case NORMAL:
                backends = Collections.singletonList(
                        ((NormalTable) table).getDataNode());
                break;
            case CUSTOM:
                throw new UnsupportedOperationException("unsupport custom table");
        }
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("targetName", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("schemaName", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("tableName", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("dbIndex", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("tableIndex", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("index", JDBCType.VARCHAR);

        for (Partition partition : backends) {
            String targetName = partition.getTargetName();
            String schemaName = partition.getSchema();
            String tableName = partition.getTable();

            Integer dbIndex = partition.getDbIndex();
            Integer tableIndex = partition.getTableIndex();
            Integer index = partition.getIndex();

            resultSetBuilder.addObjectRowPayload(
                    Arrays.asList(targetName, schemaName, tableName, dbIndex, tableIndex, index));
        }
        return response.sendResultSet(resultSetBuilder.build());
    }

    private Future<Void> showSlowSql(Response response, String body) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("trace_id", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("sql", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("sql_rows", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("start_time", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("end_time", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("execute_time", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("target_name", JDBCType.VARCHAR);
        Stream<SqlEntry> sqlRecords = SqlRecorderRuntime.INSTANCE.getRecords().stream()
                .sorted(Comparator.comparingLong(SqlEntry::getSqlTime).reversed());
        Map map = JsonUtil.from(body, Map.class);
        Object idText = map.get("trace_id");

        if (idText != null) {
            sqlRecords = sqlRecords.filter(i -> idText == i.getTraceId());
        }
        sqlRecords.forEach(r -> {
            resultSetBuilder.addObjectRowPayload(Arrays.asList(
                    Objects.toString(r.getTraceId()),
                    Objects.toString(r.getSql()),
                    Objects.toString(r.getAffectRow()),
                    Objects.toString(r.getResponseTime().minus(Duration.ofMillis(r.getSqlTime()))),
                    Objects.toString(r.getResponseTime()),
                    Objects.toString(r.getSqlTime()),
                    Objects.toString(null)
            ));
        });
        return response.sendResultSet(resultSetBuilder.build());
    }

    private Future<Void> setUserDialect(Response response, String body) throws Exception {
        MycatRouterConfigOps ops = ConfigUpdater.getOps();
        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        Map map = JsonUtil.from(body, Map.class);
        String username = (String) map.get("username");
        String dbType = (String) map.get("dialect");
        UserConfig userInfo = authenticator.getUserInfo(username);
        if (userInfo == null) {
            return response.sendError("unknown username:" + username, MySQLErrorCode.ER_UNKNOWN_ERROR);
        }
        userInfo.setDialect(dbType);
        ops.putUser(userInfo);
        ops.commit();
        return response.sendOk();
    }

    @NotNull
    private PromiseInternal<Void> loaddata(MycatDataContext dataContext, Response response, String body, MetadataManager metadataManager) throws IOException {
        Map<String, Object> map = JsonUtil.from(body, Map.class);
        String schemaName = Objects.requireNonNull((String) map.get("schemaName"));
        String tableName = Objects.requireNonNull((String) map.get("tableName"));
        final String fileName = Objects.toString(map.get("fileName"));
        CSVFormat format = CSVFormat.MYSQL;
        final Character delimiter = map.getOrDefault("delimiter", format.getDelimiter() + "")
                .toString()
                .charAt(0);
        final Character quoteChar = map.getOrDefault("quoteChar", format.getQuoteCharacter() + "")
                .toString()
                .charAt(0);
        final QuoteMode quoteMode = QuoteMode.valueOf(map.getOrDefault("quoteMode",
                        format.getQuoteMode() + "")
                .toString());
        final Character commentStart = Optional.ofNullable(map.get("commentStart")).map(c ->
                c.toString()
                        .charAt(0)).orElse(format.getCommentMarker());
        final Character escape = map.getOrDefault("escape", format.getEscapeCharacter())
                .toString()
                .charAt(0);
        final Boolean ignoreSurroundingSpaces = Boolean.parseBoolean(map.getOrDefault("escape", format.getIgnoreSurroundingSpaces())
                .toString());

        final Boolean ignoreEmptyLines = Boolean.parseBoolean(map.getOrDefault("ignoreEmptyLines", format.getIgnoreEmptyLines())
                .toString());
        final String recordSeparator = map.getOrDefault("recordSeparator", format.getRecordSeparator())
                .toString();
        final String nullString = map.getOrDefault("nullString", format.getNullString())
                .toString();
        final List headerComments = (List) map.getOrDefault("headerComments", Arrays.asList(
                Optional.ofNullable(format.getHeaderComments()).orElse(new String[]{})));
        final List<String> header = Optional.ofNullable((List<String>) map.get("header")).orElse(null);
        final Boolean skipHeaderRecord = Boolean.parseBoolean(map.getOrDefault("skipHeaderRecord",
                format.getSkipHeaderRecord()).toString());
        final Boolean allowMissingColumnNames = Boolean.parseBoolean(map.getOrDefault("allowMissingColumnNames",
                format.getSkipHeaderRecord()).toString());
        final Boolean ignoreHeaderCase = Boolean.parseBoolean(map.getOrDefault("ignoreHeaderCase",
                format.getSkipHeaderRecord()).toString());
        final Boolean trim = Boolean.parseBoolean(map.getOrDefault("trim",
                format.getSkipHeaderRecord()).toString());
        final Boolean trailingDelimiter = Boolean.parseBoolean(map.getOrDefault("trailingDelimiter",
                format.getSkipHeaderRecord()).toString());
        final Boolean autoFlush = Boolean.parseBoolean(map.getOrDefault("autoFlush",
                format.getSkipHeaderRecord()).toString());
        final Boolean allowDuplicateHeaderNames = Boolean.parseBoolean(map.getOrDefault("allowDuplicateHeaderNames",
                format.getSkipHeaderRecord()).toString());
        format = CSVFormat.newFormat(delimiter)
                .withQuote(quoteChar)
                .withQuoteMode(quoteMode)
                .withCommentMarker(commentStart)
                .withEscape(escape)
                .withIgnoreSurroundingSpaces(ignoreSurroundingSpaces)
                .withIgnoreEmptyLines(ignoreEmptyLines)
                .withRecordSeparator(recordSeparator)
                .withNullString(nullString)
                .withHeaderComments(headerComments)
                .withHeader(Optional.ofNullable(header).map(n -> n.toArray(new String[]{})).orElse(null))
                .withSkipHeaderRecord(skipHeaderRecord)
                .withAllowMissingColumnNames(allowMissingColumnNames)
                .withIgnoreHeaderCase(ignoreHeaderCase)
                .withTrim(trim)
                .withTrailingDelimiter(trailingDelimiter)
                .withAutoFlush(autoFlush)
                .withAllowDuplicateHeaderNames(allowDuplicateHeaderNames);


        MySqlInsertStatement mySqlInsertStatement = new MySqlInsertStatement();
        mySqlInsertStatement.setTableName(new SQLIdentifierExpr(tableName));
        mySqlInsertStatement.getTableSource().setSchema(schemaName);
        List<String> columnNames;
        if (header == null || header.isEmpty()) {
            TableHandler table = Objects.requireNonNull(
                    metadataManager.getTable(schemaName, tableName));
            columnNames = table.getColumns().stream().map(i -> (i.getColumnName())).collect(Collectors.toList());
        } else {
            columnNames = header;
        }
        for (SQLIdentifierExpr columnName : columnNames.stream().map(i -> new SQLIdentifierExpr("`" + i + "`")).collect(Collectors.toList())) {
            mySqlInsertStatement.addColumn(columnName);
        }
        int batch = 1000;

        Reader in = new FileReader(fileName);
        Iterable<CSVRecord> records = format.parse(in);
        Stream<SQLInsertStatement> insertStatementStream = StreamSupport.stream(records.spliterator(), false)
                .map(strings -> {
                    SQLInsertStatement sqlInsertStatement = mySqlInsertStatement.clone();
                    SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();
                    for (String string : strings) {
                        valuesClause.addValue(new SQLCharExpr(string));
                    }
                    sqlInsertStatement.addValueCause(valuesClause);
                    return sqlInsertStatement;
                });

        UnmodifiableIterator<List<VertxExecuter.EachSQL>> iterator = Iterators.partition(insertStatementStream.flatMap(statement -> {
            DrdsSqlWithParams drdsSql = DrdsRunnerHelper.preParse(statement, dataContext.getDefaultSchema());
            Iterable<VertxExecuter.EachSQL> eachSQLS1 = VertxExecuter.explainInsert(drdsSql.getParameterizedSQL(), drdsSql.getParams());
            return StreamSupport.stream(eachSQLS1.spliterator(), false);
        }).iterator(), batch);


        Future<long[]> continution = Future.succeededFuture(new long[]{0, 0});

        while (iterator.hasNext()) {
            Iterable<VertxExecuter.EachSQL> eachSQL = VertxExecuter.rewriteInsertBatchedStatements(iterator.next());
            continution = continution.flatMap(o -> {
                Future<long[]> future = VertxExecuter.simpleUpdate(dataContext, true, true, false, eachSQL);
                return future.map(o2 -> new long[]{o[0] + o2[0], Math.max(o[1], o2[1])});
            });
        }
        continution.onComplete(event -> JdbcUtils.close(in));
        return VertxUtil.castPromise(continution.flatMap(result -> {
            return response.sendOk(result[0], result[1]);
        }));
    }

    private Future<Void> showErGroup(Response response, MetadataManager metadataManager) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("groupId", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("schemaName", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("tableName", JDBCType.VARCHAR);

        Map<String, List<ShardingTable>> erTableGroup = metadataManager.getErTableGroup();
        Integer index = 0;
        for (Map.Entry<String, List<ShardingTable>> e : erTableGroup.entrySet()) {
            String key = e.getKey();
            Iterator<ShardingTable> iterator = e.getValue().iterator();
            while (iterator.hasNext()) {
                ShardingTable table = iterator.next();
                String schemaName = table.getSchemaName();
                String tableName = table.getTableName();
                resultSetBuilder.addObjectRowPayload(Arrays.asList(index.toString(), schemaName, tableName));
            }
            index++;
        }
        return response.sendResultSet(resultSetBuilder.build());
    }

    public static RowBaseIterator showClusters(String clusterName) {
        MycatRouterConfig routerConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("SWITCH_TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("MAX_REQUEST_COUNT", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("WRITE_DS", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("READ_DS", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("WRITE_L", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("READ_L", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("AVAILABLE", JDBCType.VARCHAR);
        Collection<ReplicaSelector> values = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class).getReplicaMap().values();

        Map<String, ClusterConfig> clusterConfigMap = routerConfig.getClusters().stream()
                .collect(Collectors.toMap(k -> k.getName(), v -> v));

        for (ReplicaSelector value :
                values.stream().filter(v -> {
                    if (clusterName != null) {
                        return clusterName.equalsIgnoreCase(v.getName());
                    }
                    return true;
                }).collect(Collectors.toList())
        ) {
            String NAME = value.getName();


            Optional<ClusterConfig> e = Optional.ofNullable(clusterConfigMap.get(NAME));

            ReplicaSwitchType SWITCH_TYPE = value.getSwitchType();
            int MAX_REQUEST_COUNT = value.maxRequestCount();
            String TYPE = value.getBalanceType().name();
            String WRITE_DS = ((List<PhysicsInstance>) value.getWriteDataSourceByReplicaType()).stream().map(i -> i.getName()).collect(Collectors.joining(","));
            String READ_DS = (value.getReadDataSourceByReplica()).stream().map(i -> i.getName()).collect(Collectors.joining(","));
            String WL = Optional.ofNullable(value.getDefaultWriteLoadBalanceStrategy()).map(i -> i.getClass().getName()).orElse(null);
            String RL = Optional.ofNullable(value.getDefaultReadLoadBalanceStrategy()).map(i -> i.getClass().getName()).orElse(null);
            String AVAILABLE = Boolean.toString(((List<PhysicsInstance>) value.getWriteDataSourceByReplicaType()).stream().anyMatch(PhysicsInstance::isAlive));

            resultSetBuilder.addObjectRowPayload(
                    Arrays.asList(NAME, SWITCH_TYPE, MAX_REQUEST_COUNT, TYPE,
                            WRITE_DS, READ_DS,
                            WL, RL, AVAILABLE
                    ));
        }
        return resultSetBuilder.build();
    }

    public static void mycatDmlHandler(String cmd, String body) throws Exception {
        if ("createTable".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                CreateTableConfig createTableConfig = JsonUtil.from(body, CreateTableConfig.class);
                ops.putTable(createTableConfig);
                ops.commit();
            }
            return;
        }
        if ("dropTable".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                Map map = JsonUtil.from(body, Map.class);
                String schemaName = (String) map.get("schemaName");
                String tableName = (String) map.get("tableName");
                ops.removeTable(schemaName, tableName);
                ops.commit();
            }
            return;
        }
        if ("createDataSource".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putDatasource(JsonUtil.from(body, DatasourceConfig.class));
                ops.commit();
            }
            return;
        }
        if ("dropDataSource".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.removeDatasource(JsonUtil.from(body, DatasourceConfig.class).getName());
                ops.commit();
            }
            return;
        }
        if ("createUser".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putUser(JsonUtil.from(body, UserConfig.class));
                ops.commit();
            }
            return;
        }
        if ("dropUser".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.deleteUser(JsonUtil.from(body, UserConfig.class).getUsername());
                ops.commit();
            }
            return;
        }
        if ("createCluster".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putReplica(JsonUtil.from(body, ClusterConfig.class));
                ops.commit();
            }
            return;
        }
        if ("dropCluster".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.removeReplica(JsonUtil.from(body, ClusterConfig.class).getName());
                ops.commit();
            }
            return;
        }
        if ("setSequence".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putSequence(JsonUtil.from(body, SequenceConfig.class));
                ops.commit();
            }
            return;
        }
        if ("createSchema".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putSchema(JsonUtil.from(body, LogicSchemaConfig.class));
                ops.commit();
            }
            return;
        }
        if ("dropSchema".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.dropSchema(JsonUtil.from(body, LogicSchemaConfig.class).getSchemaName());
                ops.commit();
            }
            return;
        }
        if ("repairPhysicalTable".equalsIgnoreCase(cmd)) {
            MetaClusterCurrent.wrapper(MetadataManager.class).createPhysicalTables();
            return;
        }
        if ("readXARecoveryLog".equalsIgnoreCase(cmd)) {
            XaLog xaLog = MetaClusterCurrent.wrapper(XaLog.class);
            xaLog.readXARecoveryLog();
            return;
        }
        if ("syncConfigFromFileToDb".equalsIgnoreCase(cmd)) {
            StorageManager assembleMetadataStorageManager = MetaClusterCurrent.wrapper(StorageManager.class);
            assembleMetadataStorageManager.syncToNet();
            return;
        }
        if ("loadConfigFromFile".equalsIgnoreCase(cmd)) {
            ConfigUpdater.loadConfigFromFile();
            return;
        }
        if ("syncConfigFromDbToFile".equalsIgnoreCase(cmd)) {
            StorageManager assembleMetadataStorageManager = MetaClusterCurrent.wrapper(StorageManager.class);
            assembleMetadataStorageManager.syncFromNet();
            return;
        }
    }
}
