package io.mycat.sqlhandler;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcUtils;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.google.common.base.Strings;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import groovy.transform.ToString;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.Partition;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.NameMap;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;
import io.reactivex.rxjava3.core.FlowableOnSubscribe;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class BinlogUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogUtil.class);
    public static CopyOnWriteArrayList<BinlogScheduler> schedulers = new CopyOnWriteArrayList();

    public static void register(BinlogScheduler scheduler) {
        schedulers.add(scheduler);
    }

    public static RowBaseIterator binlogSnapshot(String snapshotName) {
        String id = UUID.randomUUID().toString();
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("Id", JDBCType.VARCHAR);
        builder.addColumnInfo("Name", JDBCType.VARCHAR);
        builder.addColumnInfo("Datasource", JDBCType.VARCHAR);
        builder.addColumnInfo("File", JDBCType.VARCHAR);
        builder.addColumnInfo("Position", JDBCType.BIGINT);
        builder.addColumnInfo("Binlog_Do_DB", JDBCType.VARCHAR);
        builder.addColumnInfo("Binlog_Ignore_DB", JDBCType.VARCHAR);
        builder.addColumnInfo("Executed_Gtid_Set", JDBCType.VARCHAR);
        builder.addColumnInfo("CreateDatetime", JDBCType.VARCHAR);

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        List<String> names = new ArrayList<>(jdbcConnectionManager.getConfigAsMap().keySet());

        List<DefaultConnection> connectionList = new ArrayList<>();
        try {
            for (String name : names) {
                DefaultConnection defaultConnection = jdbcConnectionManager.getConnection(name);
                connectionList.add(defaultConnection);
            }

            for (DefaultConnection defaultConnection : connectionList) {
                Connection connection = defaultConnection.getRawConnection();
                List<Map<String, Object>> show_master_status = JdbcUtils.executeQuery(connection, "show master status", Collections.emptyList());
                Map<String, Object> map = new HashMap<>(show_master_status.get(0));
                builder.addObjectRowPayload(Arrays.asList(
                        id,
                        snapshotName,
                        defaultConnection.getDataSource().getName(),
                        map.get("File"),
                        map.get("Position"),
                        map.get("Binlog_Do_DB"),
                        map.get("Binlog_Ignore_DB"),
                        map.get("Executed_Gtid_Set"),
                        new Date()
                ));
            }
            RowBaseIterator rowBaseIterator = builder.build();


            MySqlInsertStatement mySqlInsertStatement = new MySqlInsertStatement();

            mySqlInsertStatement.setTableName(new SQLIdentifierExpr("`ds_binlog`"));
            mySqlInsertStatement.getTableSource().setSchema(("`mycat`"));

            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`Id`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`Name`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`Datasource`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`File`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`Position`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`Binlog_Ignore_DB`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`Binlog_Do_DB`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`Executed_Gtid_Set`"));
            mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`CreateDatetime`"));

            while (rowBaseIterator.next()) {
                Object[] objects = rowBaseIterator.getObjects();
                SQLInsertStatement.ValuesClause valuesClause = new SQLInsertStatement.ValuesClause();
                for (Object object : objects) {
                    valuesClause.addValue(SQLExprUtils.fromJavaObject(object));
                }
                mySqlInsertStatement.getValuesList().add(valuesClause);
            }

            try (DefaultConnection prototypeConnection = jdbcConnectionManager.getConnection(MetadataManager.getPrototype());) {
                JdbcUtils.execute(prototypeConnection.getRawConnection(), mySqlInsertStatement.toString());
            } catch (Throwable e) {
                LOGGER.error("binlogSnapshot:{} record fail.", snapshotName, e);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            for (DefaultConnection defaultConnection : connectionList) {
                defaultConnection.close();
            }
        }
        return builder.build();
    }


    public static RowBaseIterator list(List<BinlogScheduler> schedulers) {
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("ID", JDBCType.VARCHAR);
        builder.addColumnInfo("NAME", JDBCType.VARCHAR);
        builder.addColumnInfo("INFO", JDBCType.VARCHAR);
        builder.addColumnInfo("START_TIME", JDBCType.TIMESTAMP);
        builder.addColumnInfo("CONNECTION_IDS", JDBCType.VARCHAR);
        for (BinlogScheduler scheduler : schedulers) {
            String id = scheduler.getId();
            String name = scheduler.getName();
            Map<String, Map<String, List<Partition>>> listMap = scheduler.getListMap();
            LocalDateTime startTime = scheduler.getStartTime();
            String connection_ids = scheduler.getBinlogResArrayList().stream().map(i -> i.getConnectionId() + "").collect(Collectors.joining(","));
            builder.addObjectRowPayload(Arrays.asList(id, name, listMap.toString(), startTime,connection_ids));
        }
        return builder.build();
    }

    public static RowBaseIterator list() {
        return list(schedulers);
    }

    public static synchronized void clear() {
        for (BinlogScheduler scheduler : schedulers) {
            stop(scheduler.getId());
        }
        schedulers.clear();
    }

    public static boolean stop(String id) {
        return schedulers.removeIf(binlogScheduler -> {
            boolean b = binlogScheduler.getId().equalsIgnoreCase(id);
            if (b) {
                for (MigrateUtil.MigrateController controller : binlogScheduler.getControllers()) {
                    controller.stop();
                }
            }
            return b;
        });
    }

    @Data
    @AllArgsConstructor
    public static class ParamSQL {
        String sql;
        List<Object> params;

        public static ParamSQL of(
                String sql,
                List<Object> params
        ) {
            return new ParamSQL(sql, params);
        }
    }

    @Getter
    @ToString
    @AllArgsConstructor
    public static class BinlogScheduler {
        String id;
        String name;
        Map<String, Map<String, List<Partition>>> listMap;
        Future<Void> future;
        LocalDateTime startTime;
        List<MigrateUtil.MigrateController> controllers;
        List<BinlogRes> binlogResArrayList;
        public static BinlogScheduler of(
                String id,
                String name,
                Map<String, Map<String, List<Partition>>> listMap,
                List<MigrateUtil.MigrateController> controllers,
                List<BinlogRes> binlogResArrayList) {
            return new BinlogScheduler(id, name, listMap,
                    CompositeFuture.join(controllers.stream().map(i -> i.getFuture()).collect(Collectors.toList())).mapEmpty(),
                    LocalDateTime.now(),
                    controllers,
                    binlogResArrayList);
        }

    }

    @Data
    public static class BinlogArgs{
        private long connectTimeout;
        private volatile String binlogFilename;
        private volatile long binlogPosition;
    }
    @Data
    @ToString
    public static class BinlogRes{
        private long connectionId;
    }


    public static Flowable<ParamSQL> observe(BinlogArgs binlogArgs,BinlogRes binlogRes,String targetName, List<Partition> partitions) {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);

        DatasourceConfig datasourceConfig = jdbcConnectionManager.getConfigAsMap().get(targetName);

        ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(datasourceConfig.getUrl());
        HostInfo hostInfo = connectionUrlParser.getHosts().get(0);

        int port = hostInfo.getPort();
        String host = hostInfo.getHost();
        String username = datasourceConfig.getUser();
        String password = datasourceConfig.getPassword();

        BinaryLogClient client = new BinaryLogClient(
                host,
                port,
                username,
                password
        );
        client.setBlocking(true);

        if (binlogArgs!=null){
            client.setBinlogFilename(binlogArgs.getBinlogFilename());
            client.setBinlogPosition(binlogArgs.getBinlogPosition());
            client.setConnectTimeout(binlogArgs.getConnectTimeout());
        }

        NameMap<NameMap<Boolean>> filterMap = new NameMap<>();

        for (Partition partition : partitions) {
            String schema = partition.getSchema();
            String table = partition.getTable();
            NameMap<Boolean> map = filterMap.computeIfAbsent(SQLUtils.normalize(schema), s -> new NameMap<>());
            map.put(table, Boolean.TRUE);
        }


        return Flowable.create(new FlowableOnSubscribe<ParamSQL>() {
            @Override
            public void subscribe(@NonNull FlowableEmitter<ParamSQL> emitter) throws Throwable {
                emitter.setDisposable(Disposable.fromAction(() -> {
                    if (client.isConnected()) {
                        client.disconnect();
                    }
                }));
                try {
                    client.registerEventListener(new BinaryLogClient.EventListener() {
                        private final Map<Long, TableMapEventData> tablesById = new HashMap<Long, TableMapEventData>();
                        private final Map<String, Map<Integer, Map<String, Object>>> tablesColumnMap = new HashMap<>();
                        private String binlogFilename;
                        private Charset charset = StandardCharsets.UTF_8;

                        private Map<Integer, Map<String, Object>> loadColumn(String database, String table) {
                            Map<Integer, Map<String, Object>> res = new HashMap<>();
                            try (DefaultConnection defaultConnection = jdbcConnectionManager.getConnection(targetName)) {
                                List<Map<String, Object>> list = JdbcUtils.executeQuery(defaultConnection.getRawConnection(),
                                        "select  COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, CHARACTER_SET_NAME from INFORMATION_SCHEMA.COLUMNS where table_name='" + table + "' and TABLE_SCHEMA='" + database + "'",
                                        Collections.emptyList());
                                for (Map<String, Object> stringObjectMap : list) {
                                    Number pos = (Number) stringObjectMap.get("ORDINAL_POSITION");
                                    res.put(pos.intValue(), stringObjectMap);
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                            return res;
                        }

                        @Override
                        public void onEvent(Event event) {
                            try {
                                EventHeader header = event.getHeader();
                                switch (header.getEventType()) {
                                    case UNKNOWN:
                                        break;
                                    case START_V3:
                                        break;
                                    case QUERY:
                                        QueryEventData queryEventData = event.getData();
                                        String query = queryEventData.getSql();
                                        if (!query.startsWith("S")) {
                                            handleDDL(queryEventData);
                                        }
                                        break;
                                    case STOP:
                                        emitter.onComplete();
                                        break;
                                    case ROTATE: {
                                        RotateEventData data = event.getData();
                                        this.binlogFilename = data.getBinlogFilename();
                                        break;
                                    }
                                    case INTVAR:
                                        break;
                                    case LOAD:
                                        break;
                                    case SLAVE:
                                        break;
                                    case CREATE_FILE:
                                        break;
                                    case APPEND_BLOCK:
                                        break;
                                    case EXEC_LOAD:
                                        break;
                                    case DELETE_FILE:
                                        break;
                                    case NEW_LOAD:
                                        break;
                                    case RAND:
                                        break;
                                    case USER_VAR:
                                        break;
                                    case FORMAT_DESCRIPTION:
                                        break;
                                    case XID:
                                        break;
                                    case BEGIN_LOAD_QUERY:
                                        break;
                                    case EXECUTE_LOAD_QUERY:
                                        break;
                                    case TABLE_MAP: {
                                        handleTableMap(event);
                                        break;
                                    }
                                    case PRE_GA_WRITE_ROWS:
                                    case WRITE_ROWS:
                                    case EXT_WRITE_ROWS:
                                        handleWriteRowsEvent(event);
                                        break;
                                    case EXT_UPDATE_ROWS:
                                    case PRE_GA_UPDATE_ROWS:
                                    case UPDATE_ROWS:
                                        handleUpdateRowsEvent(event);
                                        break;
                                    case PRE_GA_DELETE_ROWS:
                                    case EXT_DELETE_ROWS:
                                    case DELETE_ROWS:
                                        handleDeleteRowsEvent(event);
                                        break;
                                    case INCIDENT:
                                        break;
                                    case HEARTBEAT:
                                        break;
                                    case IGNORABLE:
                                        break;
                                    case ROWS_QUERY:
                                        break;
                                    case GTID:
                                        break;
                                    case ANONYMOUS_GTID:
                                        break;
                                    case PREVIOUS_GTIDS:
                                        break;
                                    case TRANSACTION_CONTEXT:
                                        break;
                                    case VIEW_CHANGE:
                                        break;
                                    case XA_PREPARE:
                                        break;
                                }
                            } catch (Exception e) {
                                emitter.tryOnError(e);
                            }
                        }

                        private void handleDDL(QueryEventData event) {
                            String sql = event.getSql();
                            String database = event.getDatabase();
                            if ("BEGIN".equalsIgnoreCase(sql)) {
                                return;
                            }
                            try {
                                SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
                                if (sqlStatement instanceof SQLDDLStatement) {
                                    SQLDDLStatement sqlDdlStatement = (SQLDDLStatement) sqlStatement;
                                    MySqlSchemaStatVisitor mySqlSchemaStatVisitor = new MySqlSchemaStatVisitor();
                                    sqlDdlStatement.accept(mySqlSchemaStatVisitor);
                                    Map<TableStat.Name, TableStat> tables = mySqlSchemaStatVisitor.getTables();
                                    boolean meet = tables.keySet().stream().allMatch(i -> filter(database, i.getName()));
                                }
                            } catch (Throwable throwable) {
                                LOGGER.error("sql:{}", sql, throwable);
                            }
                        }

                        private void handleUpdateRowsEvent(Event event) {
                            try {
                                UpdateRowsEventData eventData = event.getData();
                                BitSet includedColumnsBeforeUpdate = eventData.getIncludedColumnsBeforeUpdate();
                                BitSet includedColumns = eventData.getIncludedColumns();
                                List<Map.Entry<Serializable[], Serializable[]>> rows = eventData.getRows();
                                TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
                                if (!filter(tableMapEvent.getDatabase(), tableMapEvent.getTable()))
                                    return;

                                MySqlUpdateStatement mySqlUpdateStatement = new MySqlUpdateStatement();
                                SQLExprTableSource sqlTableSource = new SQLExprTableSource();
                                sqlTableSource.setExpr("`" + tableMapEvent.getTable() + "`");
                                sqlTableSource.setSchema("`" + tableMapEvent.getDatabase() + "`");
                                mySqlUpdateStatement.setTableSource(sqlTableSource);
                                Map<Integer, Map<String, Object>> xxx = tablesColumnMap.get(tableMapEvent.getDatabase() + "." + tableMapEvent.getTable());
                                SQLBinaryOpExprGroup sqlBinaryOpExprGroup = getCondition(includedColumnsBeforeUpdate, xxx);

                                for (int i = 0; i < includedColumns.length(); i++) {
                                    int column = includedColumns.nextSetBit(i);
                                    Map<String, Object> coumnMap = xxx.get(column + 1);
                                    Object column_name = coumnMap.get("COLUMN_NAME");
                                    SQLUpdateSetItem sqlUpdateSetItem = new SQLUpdateSetItem();
                                    sqlUpdateSetItem.setColumn(new SQLIdentifierExpr("`" + column_name + "`"));
                                    sqlUpdateSetItem.setValue(new SQLVariantRefExpr("?"));
                                    mySqlUpdateStatement.getItems().add(sqlUpdateSetItem);
                                }

                                mySqlUpdateStatement.setWhere(sqlBinaryOpExprGroup);
//                                mySqlUpdateStatement.setLimit(new SQLLimit(1));
                                sqlUpdate(mySqlUpdateStatement.toString(), rows);
                            } catch (Throwable throwable) {
                                LOGGER.error("{}", event, throwable);
                            }
                        }

                        private SQLBinaryOpExprGroup getCondition(BitSet includedColumnsBeforeUpdate, Map<Integer, Map<String, Object>> xxx) {
                            SQLBinaryOpExprGroup sqlBinaryOpExprGroup = new SQLBinaryOpExprGroup(SQLBinaryOperator.BooleanAnd);

                            for (int i = 0; i < includedColumnsBeforeUpdate.length(); i++) {
                                int column = includedColumnsBeforeUpdate.nextSetBit(i);
                                Map<String, Object> coumnMap = xxx.get(column + 1);
                                Object column_name = coumnMap.get("COLUMN_NAME");
                                SQLExpr sqlExpr = SQLUtils.toSQLExpr("`" + column_name + "` <=> ?");
                                sqlBinaryOpExprGroup.add(sqlExpr);
                            }
                            return sqlBinaryOpExprGroup;
                        }

                        private void sqlUpdate(String toString, List<Map.Entry<Serializable[], Serializable[]>> rows) {
                            try {
                                for (Map.Entry<Serializable[], Serializable[]> row : rows) {

                                    Serializable[] key = row.getKey();
                                    Serializable[] value = row.getValue();

                                    ArrayList<Object> objects = new ArrayList<>(key.length + value.length);
                                    Collections.addAll(objects, value);
                                    Collections.addAll(objects, key);

                                    emitter.onNext(ParamSQL.of(toString, objects));
                                }
                            } catch (Exception e) {
                                LOGGER.error("", e);
                                emitter.tryOnError(e);
                            }
                        }

                        private boolean filter(String database, String table) {
                            if (Strings.isNullOrEmpty(database)) {
                                return false;
                            }
                            if (Strings.isNullOrEmpty(table)) {
                                return false;
                            }
                            NameMap<Boolean> tableMap = filterMap.get(SQLUtils.normalize(database), false);
                            if (tableMap != null && !tableMap.map().isEmpty()) {
                                return Boolean.TRUE.equals(tableMap.get(SQLUtils.normalize(table)));
                            }
                            return false;
                        }

                        private void handleTableMap(Event event) {
                            TableMapEventData tableMapEventData = event.getData();
                            tablesById.put(tableMapEventData.getTableId(), tableMapEventData);
                            String tableName = tableMapEventData.getDatabase() + "." + tableMapEventData.getTable();
                            if (!tablesColumnMap.containsKey(tableName)) {
                                tablesColumnMap.put(tableName, loadColumn(tableMapEventData.getDatabase(), tableMapEventData.getTable()));
                            }
                        }

                        private void handleWriteRowsEvent(Event event) {
                            try {
                                WriteRowsEventData eventData = event.getData();
                                TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
                                if (!filter(tableMapEvent.getDatabase(), tableMapEvent.getTable()))
                                    return;
                                Map<Integer, Map<String, Object>> xxx = tablesColumnMap.get(tableMapEvent.getDatabase() + "." + tableMapEvent.getTable());
                                BitSet inculudeColumn = eventData.getIncludedColumns();


                                MySqlInsertStatement mySqlInsertStatement = new MySqlInsertStatement();
                                mySqlInsertStatement.setTableName(new SQLIdentifierExpr("`" + tableMapEvent.getTable() + "`"));
                                mySqlInsertStatement.getTableSource().setSchema("`" + tableMapEvent.getDatabase() + "`");
                                SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
                                mySqlInsertStatement.setValues(values);
                                int size = inculudeColumn.length();
                                List<Serializable[]> rows = eventData.getRows();
                                for (int i = 0; i < size; i++) {
                                    int column = inculudeColumn.nextSetBit(i);
                                    Map<String, Object> coumnMap = xxx.get(column + 1);
                                    Object column_name = coumnMap.get("COLUMN_NAME");
                                    mySqlInsertStatement.addColumn(new SQLIdentifierExpr("`" + column_name + "`"));
                                    values.addValue(new SQLVariantRefExpr("?"));
                                }
                                sqlInsert(mySqlInsertStatement.toString(), rows);
                            } catch (Throwable throwable) {
                                LOGGER.error("{}", event, throwable);
                            }
                        }

                        private void sqlInsert(String sql, List<Serializable[]> rows) {
                            for (Serializable[] row : rows) {
                                emitter.onNext(ParamSQL.of(sql, Arrays.asList(row)));
                            }
                        }

                        private void handleDeleteRowsEvent(Event event) {
                            try {
                                DeleteRowsEventData eventData = event.getData();
                                EventHeader header = event.getHeader();

                                TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
                                if (!filter(tableMapEvent.getDatabase(), tableMapEvent.getTable()))
                                    return;
                                Map<Integer, Map<String, Object>> xxx = tablesColumnMap.get(tableMapEvent.getDatabase() + "." + tableMapEvent.getTable());
                                BitSet inculudeColumn = eventData.getIncludedColumns();
                                SQLBinaryOpExprGroup condition = getCondition(inculudeColumn, xxx);
                                int size = inculudeColumn.length();
                                List<Serializable[]> rows = eventData.getRows();

                                MySqlDeleteStatement mySqlDeleteStatement = new MySqlDeleteStatement();
                                //mySqlDeleteStatement.setLimit(new SQLLimit(1));
                                SQLExprTableSource sqlExprTableSource = new SQLExprTableSource();

                                sqlExprTableSource.setExpr("`" + tableMapEvent.getTable() + "`");
                                sqlExprTableSource.setSchema("`" + tableMapEvent.getDatabase() + "`");

                                mySqlDeleteStatement.setTableSource(sqlExprTableSource);

                                mySqlDeleteStatement.addWhere(condition);
                                sqlDelete(mySqlDeleteStatement.toString(), rows);
                            } catch (Throwable throwable) {
                                LOGGER.error("{}", event, throwable);
                            }
                        }

                        private void sqlDelete(String mySqlDeleteStatement, List<Serializable[]> rows) {
                            for (Serializable[] row : rows) {
                                emitter.onNext(ParamSQL.of(mySqlDeleteStatement, Arrays.asList(row)));
                            }
                        }

                    });
                    client.connect(5000);
                    binlogRes.setConnectionId(client.getConnectionId());
                } catch (Throwable throwable) {
                    emitter.tryOnError(throwable);
                }
            }
        }, BackpressureStrategy.BUFFER);
    }
}
