package io.mycat.sqlhandler.dql;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.google.common.collect.Iterables;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.commands.MycatdbCommand;
import io.mycat.commands.SqlResultSetService;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStatus;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.SqlHints;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import io.mycat.util.JsonUtil;
import io.mycat.util.NameMap;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.vertx.core.Future;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import java.io.FileReader;
import java.io.Reader;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HintHandler extends AbstractSQLHandler<MySqlHintStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlHintStatement> request, MycatDataContext dataContext, Response response) {
        MySqlHintStatement ast = request.getAst();
        List<SQLCommentHint> hints = ast.getHints();
        try {
            if (hints.size() == 1) {
                String s = SqlHints.unWrapperHint(hints.get(0).getText());
                if (s.startsWith("mycat:")) {
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


                    MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                    MycatRouterConfig routerConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
                    ReplicaSelectorRuntime replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
                    JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    MycatServer mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);

                    if ("showErGroup".equalsIgnoreCase(cmd)) {
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
                    if ("loaddata".equalsIgnoreCase(cmd)) {
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
                        try (Reader in = new FileReader(fileName)) {
                            Iterable<CSVRecord> records = format.parse(in);
                            Iterator<SQLInsertStatement> iterator = StreamSupport.stream(Iterables.partition(records, batch).spliterator(), false)
                                    .map(mrecord -> {
                                        SQLInsertStatement sqlInsertStatement = mySqlInsertStatement.clone();
                                        List<SQLInsertStatement.ValuesClause> valuesList = new ArrayList<>(batch);
                                        for (CSVRecord strings : mrecord) {
                                            SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();
                                            for (String string : strings) {
                                                valuesClause.addValue(new SQLCharExpr(string));
                                            }
                                            valuesList.add(valuesClause);
                                        }
                                        sqlInsertStatement.getValuesList().addAll(valuesList);
                                        return sqlInsertStatement;
                                    }).iterator();
                            DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
                            XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
                            Future<long[]> future = Future.succeededFuture(new long[]{0, 0});
                            while (iterator.hasNext()) {
                                SQLInsertStatement statement = iterator.next();
                                DrdsSql drdsSql = drdsRunner.preParse(statement);
                                Plan plan = drdsRunner.getPlan(dataContext, drdsSql);
                                future=  future.flatMap((l) -> VertxExecuter.runMycatInsertRel(transactionSession, dataContext,
                                        (MycatInsertRel) plan.getPhysical(), drdsSql.getParams()));
                            }
                            return VertxUtil.castPromise(future.flatMap(l -> response.sendOk(l[0], l[1])));
                        }
                    }
                    if ("setUserDialect".equalsIgnoreCase(cmd)) {
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
                    if ("showSlowSql".equalsIgnoreCase(cmd)) {
                        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
                        resultSetBuilder.addColumnInfo("trace_id", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("sql", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("sql_rows", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("start_time", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("end_time", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("execute_time", JDBCType.VARCHAR);
                        resultSetBuilder.addColumnInfo("target_name", JDBCType.VARCHAR);
                        Stream<SqlRecord> sqlRecords = SqlRecorderRuntime.INSTANCE.getRecords().stream()
                                .sorted(Comparator.comparingLong(SqlRecord::getExecuteTime).reversed());
                        Map map = JsonUtil.from(body, Map.class);
                        Object idText = map.get("trace_id");

                        if (idText != null) {
                            long id = Long.parseLong(Objects.toString(idText));
                            sqlRecords = sqlRecords.filter(i -> id == i.getId());
                        }
                        sqlRecords.forEach(r -> {
                            resultSetBuilder.addObjectRowPayload(Arrays.asList(
                                    Objects.toString(r.getId()),
                                    Objects.toString(r.getSql()),
                                    Objects.toString(r.getSqlRows()),
                                    Objects.toString(r.getStartTime()),
                                    Objects.toString(r.getEndTime()),
                                    Objects.toString(r.getExecuteTime()),
                                    Objects.toString(r.getTarget())
                            ));
                        });
                        return response.sendResultSet(resultSetBuilder.build());
                    }
                    if ("showDataNodes".equalsIgnoreCase(cmd)) {
                        Map map = JsonUtil.from(body, Map.class);
                        TableHandler table = metadataManager.getTable((String) map.get("schemaName"),
                                (String) map.get("tableName"));
                        LogicTableType type = table.getType();
                        List<DataNode> backends = null;
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

                        for (DataNode dataNode : backends) {
                            String targetName = dataNode.getTargetName();
                            String schemaName = dataNode.getSchema();
                            String tableName = dataNode.getTable();

                            resultSetBuilder.addObjectRowPayload(
                                    Arrays.asList(targetName, schemaName, tableName));
                        }
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
                        DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
                        return drdsRunner.runHbtOnDrds(dataContext, hbt, response);
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
                        List<UserConfig> userConfigs = authenticator.allUsers();
                        for (UserConfig userConfig : userConfigs) {
                            builder.addObjectRowPayload(Arrays.asList(
                                    userConfig.getUsername(),
                                    userConfig.getPassword(),
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
                                                return i.getShadingTables();
                                            case GLOBAL:
                                                return i.getGlobalTables();
                                            case NORMAL:
                                                return i.getNormalTables();
                                            case CUSTOM:
                                                return i.getCustomTables();
                                            default:
                                                return null;
                                        }
                                    }).map(i -> i.get(TABLE_NAME)).findFirst()
                                    .map(i -> i.toString()).orElse(null);
                            builder.addObjectRowPayload(Arrays.asList(SCHEMA_NAME, TABLE_NAME, CREATE_TABLE_SQL, TYPE, COLUMNS, CONFIG));
                        });
                        return response.sendResultSet(() -> builder.build());
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
                        Collection<JdbcDataSource> jdbcDataSources = connectionManager.map(i -> i.getDatasourceInfo()).map(i -> i.values()).orElse(Collections.emptyList());
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
                        Map<String, DatasourceConfig> dataSourceConfig = routerConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));

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


                        for (HeartbeatFlow heartbeatFlow : replicaSelectorRuntime.getHeartbeatDetectorMap().values()) {
                            PhysicsInstance instance = heartbeatFlow.instance();

                            String NAME = instance.getName();
                            String TYPE = instance.getType().name();
                            boolean READABLE = instance.asSelectRead();
                            int SESSION_COUNT = instance.getSessionCounter();
                            int WEIGHT = instance.getWeight();
                            boolean ALIVE = instance.isAlive();
                            boolean MASTER = instance.isMaster();

                            long SLAVE_THRESHOLD = heartbeatFlow.getSlaveThreshold();

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

                            String replicaDataSourceSelectorList = String.join(",", replicaSelectorRuntime.getRepliaNameListByInstanceName(NAME));

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
                        return response.sendResultSet(resultSetBuilder.build());
                    }
                    if ("showHeartbeatStatus".equalsIgnoreCase(cmd)) {
                        Map<String, HeartbeatFlow> heartbeatDetectorMap = replicaSelectorRuntime.getHeartbeatDetectorMap();
                        for (Map.Entry<String, HeartbeatFlow> entry : heartbeatDetectorMap.entrySet()) {
                            String key = entry.getKey();
                            HeartbeatFlow value = entry.getValue();

                            ResultSetBuilder builder = ResultSetBuilder.create();
                            builder.addColumnInfo("name", JDBCType.VARCHAR);
                            builder.addColumnInfo("status", JDBCType.VARCHAR);
                            builder.addObjectRowPayload(Arrays.asList(
                                    Objects.toString(key),
                                    Objects.toString(value.getDsStatus())
                            ));
                            return response.sendResultSet(() -> builder.build());
                        }
                    }
                    if ("showInstances".equalsIgnoreCase(cmd)) {
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
                                replicaSelectorRuntime.getPhysicsInstanceMap().values();
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


                            String replicaDataSourceSelectorList = String.join(",", replicaSelectorRuntime.getRepliaNameListByInstanceName(NAME));

                            resultSetBuilder.addObjectRowPayload(
                                    Arrays.asList(NAME, ALIVE, READABLE, TYPE, SESSION_COUNT, WEIGHT, MASTER,
                                            e.map(i -> i.getMaxCon()).orElse(-1),
                                            replicaDataSourceSelectorList
                                    ));
                        }
                        return response.sendResultSet(resultSetBuilder.build());
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

                        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                        List<NameableExecutor> nameableExecutors = Arrays.asList(mycatWorkerProcessor.getMycatWorker(),
                                mycatWorkerProcessor.getTimeWorker());

                        for (NameableExecutor w : nameableExecutors) {
                            builder.addObjectRowPayload(Arrays.asList(
                                    w.getName(),
                                    w.getPoolSize(),
                                    w.getActiveCount(),
                                    w.getQueue().size(),
                                    w.getCompletedTaskCount(),
                                    w.getTaskCount()));
                        }
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
                    mycatDmlHandler(cmd, body, ast);
                    return response.sendOk();
                }
            }
            return response.sendOk();
        } catch (Throwable throwable) {
            return response.sendError(throwable);
        }
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
        Collection<ReplicaDataSourceSelector> values = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class).getReplicaMap().values();

        Map<String, ClusterConfig> clusterConfigMap = routerConfig.getClusters().stream()
                .collect(Collectors.toMap(k -> k.getName(), v -> v));

        for (ReplicaDataSourceSelector value :
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
            String WRITE_DS = ((List<PhysicsInstance>) value.getWriteDataSource()).stream().map(i -> i.getName()).collect(Collectors.joining(","));
            String READ_DS = (value.getReadDataSource()).stream().map(i -> i.getName()).collect(Collectors.joining(","));
            String WL = Optional.ofNullable(value.getDefaultWriteLoadBalanceStrategy()).map(i -> i.getClass().getName()).orElse(null);
            String RL = Optional.ofNullable(value.getDefaultReadLoadBalanceStrategy()).map(i -> i.getClass().getName()).orElse(null);
            String AVAILABLE = Boolean.toString(((List<PhysicsInstance>) value.getWriteDataSource()).stream().anyMatch(PhysicsInstance::isAlive));

            resultSetBuilder.addObjectRowPayload(
                    Arrays.asList(NAME, SWITCH_TYPE, MAX_REQUEST_COUNT, TYPE,
                            WRITE_DS, READ_DS,
                            WL, RL, AVAILABLE
                    ));
        }
        return resultSetBuilder.build();
    }

    public static void mycatDmlHandler(String cmd, String body, MySqlHintStatement ast) throws Exception {
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
    }
}