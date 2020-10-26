package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.MetadataManager;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.router.ShardingTableHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.SqlHints;
import io.mycat.util.JsonUtil;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * chenjunwnen
 * <p>
 * 实现创建表
 */

public class CreateTableSQLHandler extends AbstractSQLHandler<MySqlCreateTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableSQLHandler.class);

    @Override
    protected void onExecute(SQLRequest<MySqlCreateTableStatement> request, MycatDataContext dataContext, Response response) {
        SQLCreateTableStatement ast = request.getAst();
        Map<String, Object> attributes = ast.getAttributes();
        String schemaName = ast.getSchema() == null ? dataContext.getDefaultSchema() : SQLUtils.normalize(ast.getSchema());
        String tableName = ast.getTableName();
        if (tableName == null) {
            response.sendError(new MycatException("CreateTableSQL need tableName"));
            return;
        }
        tableName = SQLUtils.normalize(tableName);
        String hints = (String) attributes.get(SqlHints.AFTER_COMMENT);
        String createTableSql = request.getSqlString();
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            if (hints == null) {
                ops.putNormalTable(schemaName, tableName, createTableSql);
            } else {
                Map<String, Object> infos = JsonUtil.from(hints, Map.class);
                switch (infos.get("type").toString()) {
                    case "normal": {
                        ops.putNormalTable(schemaName, tableName, createTableSql, (String) infos.get("targetName"));
                        break;
                    }
                    case "global": {
                        ops.putGlobalTable(schemaName, tableName, createTableSql);
                        break;
                    }
                    case "sharding": {
                        ops.putShardingTable(schemaName, tableName, createTableSql, infos);
                        break;
                    }
                }

            }
            ops.commit();
            response.sendOk();
        }
    }
//
//    private void backup(MycatDataContext dataContext, Response response, SQLCreateTableStatement ast) {
//        List<Throwable> throwables = new ArrayList<>();
//        try {
//            String schemaName = ast.getSchema() == null ? dataContext.getDefaultSchema() : SQLUtils.normalize(ast.getSchema());
//            String tableName = ast.getTableName();
//            if (tableName == null) {
//                response.sendError(new MycatException("CreateTableSQL need tableName"));
//                return;
//            }
//            tableName = SQLUtils.normalize(tableName);
//            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
//            TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
//            if (tableHandler == null) {
//                response.sendError(new MycatException(schemaName + "." + tableName + " is not existed"));
//                return;
//            }
//            Map<String, Set<String>> sqlAndDatasoureMap = new HashMap<>();
//            List<DataNode> databaseCollections = new ArrayList<>();
//            if (tableHandler instanceof ShardingTableHandler) {
//                ShardingTableHandler handler = (ShardingTableHandler) tableHandler;
//                for (DataNode shardingBackend : handler.dataNodes()) {
//                    makeTask(ast, sqlAndDatasoureMap, shardingBackend);
//                }
//                databaseCollections.addAll(handler.dataNodes());
//            } else if (tableHandler instanceof GlobalTableHandler) {
//                GlobalTableHandler handler = (GlobalTableHandler) tableHandler;
//                for (DataNode shardingBackend : handler.getGlobalDataNode()) {
//                    makeTask(ast, sqlAndDatasoureMap, shardingBackend);
//                }
//                databaseCollections.addAll(handler.getGlobalDataNode());
//            } else {
//                throw new UnsupportedOperationException("UnsupportedOperation :" + tableHandler);
//            }
//            List<String> dataSources = sqlAndDatasoureMap.values().stream().flatMap(i -> i.stream()).distinct().collect(Collectors.toList());
//
//            //建立物理数据中与逻辑库逻辑表同名物理库
//            SchemaInfo schemaInfo = new SchemaInfo(tableHandler.getSchemaName(), tableHandler.getTableName());
//            dataSources.forEach(i -> {
//                BackendTableInfo backendTableInfo = new BackendTableInfo(i, schemaInfo);
//                makeTask(ast, sqlAndDatasoureMap, backendTableInfo);
//            });
//
//            //创建库
//            createDatabase(throwables, databaseCollections);
//            createTable(throwables, sqlAndDatasoureMap);
//
//            if (throwables.isEmpty()) {
//                response.sendOk();
//                return;
//            } else {
//                response.sendError(new MycatException(throwables.toString()));
//                return;
//            }
//        } catch (Throwable throwable) {
//            response.sendError(throwable);
//            return;
//        }
//    }
//
//    private void createTable(List<Throwable> throwables, Map<String, Set<String>> sqlAndDatasoureMap) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
//        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
//        List<CompletableFuture> resList = new ArrayList<>();
//        sqlAndDatasoureMap.forEach((sql, dataSources) -> {
//            for (String dataSource : dataSources) {
//                resList.add(CompletableFuture.runAsync(() -> {
//                    try (DefaultConnection connection = jdbcConnectionManager.getConnection(dataSource)) {
//                        connection.executeUpdate(sql, false);
//                    }
//                }, mycatWorkerProcessor.getMycatWorker()));
//            }
//        });
//
//        CompletableFuture.allOf(resList.toArray(new CompletableFuture[0]))
//                .exceptionally(throwable -> {
//                    LOGGER.error("执行SQL失败", throwable);
//                    throwables.add(throwable);
//                    return null;
//                })
//                .get(5, TimeUnit.MINUTES);
//    }
//
//    private void createDatabase(List<Throwable> throwables, List<DataNode> databaseCollections) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
//        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
//        List<CompletableFuture> resList = new ArrayList<>();
//        for (DataNode databaseCollection : databaseCollections) {
//            for (String dataSource : getDatasource(databaseCollection.getTargetName())) {
//                resList.add(CompletableFuture.runAsync(() -> {
//                    try (DefaultConnection connection = jdbcConnectionManager.getConnection(dataSource)) {
//                        connection.executeUpdate(MessageFormat.format("create database if not exists  {0} ",
//                                dataSource), false);
//                    }
//                }, mycatWorkerProcessor.getMycatWorker()));
//            }
//        }
//
//        CompletableFuture.allOf(resList.toArray(new CompletableFuture[0]))
//                .exceptionally(throwable -> {
//                    LOGGER.error("执行SQL失败", throwable);
//                    throwables.add(throwable);
//                    return null;
//                })
//                .get(5, TimeUnit.MINUTES);
//    }
//
//
//    private void makeTask(SQLCreateTableStatement ast, Map<String, Set<String>> sqlAndDatasoureMap, DataNode shardingBackend) {
//        String targetName = shardingBackend.getTargetName();
//        SQLCreateTableStatement entry = ast.clone();
//        entry.setIfNotExiists(true);
//        entry.setTableName(shardingBackend.getTable());//设置库名 表名顺序不能乱
//        entry.setSchema(shardingBackend.getSchema());
//        if (!shardingBackend.getTable().equals(entry.getTableName())) {
//            throw new AssertionError();
//        }
//        if (!shardingBackend.getSchema().equals(entry.getSchema())) {
//            throw new AssertionError();
//        }
//
//        String sql = entry.toString();
//        Set<String> set = sqlAndDatasoureMap.computeIfAbsent(sql, s -> new HashSet<>());
//        set.addAll(getDatasource(targetName));
//    }
//
//    private static Set<String> getDatasource(String targetName) {
//        Set<String> dataSources = new HashSet<>();
//        ReplicaSelectorRuntime selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
//        if (selectorRuntime.isReplicaName(targetName)) {
//            ReplicaDataSourceSelector dataSourceSelector = selectorRuntime.getDataSourceSelector(targetName);
//            dataSources.addAll(dataSourceSelector.getRawDataSourceMap().keySet());
//        }
//        if (selectorRuntime.isDatasource(targetName)) {
//            dataSources.add(targetName);
//        }
//        return dataSources;
//    }
}
