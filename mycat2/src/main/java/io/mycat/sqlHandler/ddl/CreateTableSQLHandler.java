package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import io.mycat.BackendTableInfo;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.SchemaInfo;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.MetadataManager;
import io.mycat.router.ShardingTableHandler;
import io.mycat.TableHandler;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * chenjunwnen
 *
 * 实现创建表
 */
@Resource
public class CreateTableSQLHandler extends AbstractSQLHandler<SQLCreateTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableSQLHandler.class);

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLCreateTableStatement> request, MycatDataContext dataContext, Response response) {
        SQLCreateTableStatement ast = request.getAst();

        List<Throwable> throwables = new ArrayList<>();
        try {
            String schemaName = ast.getSchema() == null ? dataContext.getDefaultSchema() : SQLUtils.normalize(ast.getSchema());
            String tableName = ast.getTableName();
            if (tableName == null) {
                response.sendError(new MycatException("CreateTableSQL need tableName"));
                return ExecuteCode.PERFORMED;
            }
            tableName = SQLUtils.normalize(tableName);
            TableHandler tableHandler = MetadataManager.INSTANCE.getTable(schemaName, tableName);
            if (tableHandler == null) {
                response.sendError(new MycatException(schemaName + "." + tableName + " is not existed"));
                return ExecuteCode.PERFORMED;
            }
            Map<String, Set<String>> sqlAndDatasoureMap = new HashMap<>();
            List<BackendTableInfo> databaseCollections= new ArrayList<>();
            if (tableHandler instanceof ShardingTableHandler) {
                ShardingTableHandler handler = (ShardingTableHandler) tableHandler;
                for (BackendTableInfo shardingBackend : handler.getShardingBackends()) {
                    makeTask(ast, sqlAndDatasoureMap, shardingBackend);
                }
                databaseCollections.addAll(handler.getShardingBackends());
            } else if (tableHandler instanceof GlobalTableHandler) {
                GlobalTableHandler handler = (GlobalTableHandler) tableHandler;
                for (BackendTableInfo shardingBackend : handler.getDataNodeMap().values()) {
                    makeTask(ast, sqlAndDatasoureMap, shardingBackend);
                }
                databaseCollections.addAll(handler.getDataNodeMap().values());
            } else {
                throw new UnsupportedOperationException("UnsupportedOperation :" + tableHandler);
            }
            List<String> dataSources = sqlAndDatasoureMap.values().stream().flatMap(i->i.stream()).distinct().collect(Collectors.toList());

            //建立物理数据中与逻辑库逻辑表同名物理库
            SchemaInfo schemaInfo = new SchemaInfo(tableHandler.getSchemaName(),tableHandler.getTableName());
            dataSources.forEach(i->{
                BackendTableInfo backendTableInfo = new BackendTableInfo(i, schemaInfo);
                makeTask(ast, sqlAndDatasoureMap, backendTableInfo);
            });

            //创建库
            createDatabase(throwables, databaseCollections);
            createTable(throwables, sqlAndDatasoureMap);

            if (throwables.isEmpty()) {
                response.sendOk();
                return ExecuteCode.PERFORMED;
            } else {
                response.sendError(new MycatException(throwables.toString()));
                return ExecuteCode.PERFORMED;
            }
        } catch (Throwable throwable) {
            response.sendError(throwable);
            return ExecuteCode.PERFORMED;
        }
    }

    private void createTable(List<Throwable> throwables, Map<String, Set<String>> sqlAndDatasoureMap) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        List<CompletableFuture> resList = new ArrayList<>();

        sqlAndDatasoureMap.forEach((sql, dataSources) -> {
            for (String dataSource : dataSources) {
                resList.add(CompletableFuture.runAsync(() -> {
                    try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(dataSource)) {
                        connection.executeUpdate(sql, false, 0);
                    }
                },     JdbcRuntime.INSTANCE.getFetchDataExecutorService()));
            }
        });

        CompletableFuture.allOf(resList.toArray(new CompletableFuture[0]))
                .exceptionally(throwable -> {
                    LOGGER.error("执行SQL失败", throwable);
                    throwables.add(throwable);
                    return null;
                })
                .get(5, TimeUnit.MINUTES);
    }

    private void createDatabase(List<Throwable> throwables, List<BackendTableInfo> databaseCollections) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        List<CompletableFuture> resList = new ArrayList<>();
        for (BackendTableInfo databaseCollection : databaseCollections) {
            for (String dataSource : getDatasource(databaseCollection.getTargetName())) {
                resList.add(CompletableFuture.runAsync(() -> {
                    try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(dataSource)) {
                        connection.executeUpdate(MessageFormat.format("create database if not exists  {0} ",
                                dataSource), false, 0);
                    }
                },       JdbcRuntime.INSTANCE.getFetchDataExecutorService()));
            }
        }

        CompletableFuture.allOf(resList.toArray(new CompletableFuture[0]))
                .exceptionally(throwable -> {
                    LOGGER.error("执行SQL失败", throwable);
                    throwables.add(throwable);
                    return null;
                })
                .get(5, TimeUnit.MINUTES);
    }


    private void makeTask(SQLCreateTableStatement ast, Map<String, Set<String>> sqlAndDatasoureMap, BackendTableInfo shardingBackend) {
        String targetName = shardingBackend.getTargetName();
        SchemaInfo schemaInfo = shardingBackend.getSchemaInfo();
        SQLCreateTableStatement entry = ast.clone();
        entry.setIfNotExiists(true);
        entry.setTableName(schemaInfo.getTargetTable());//设置库名 表名顺序不能乱
        entry.setSchema(schemaInfo.getTargetSchema());
        if (!schemaInfo.getTargetTable().equals(entry.getTableName())) {
            throw new AssertionError();
        }
        if (!schemaInfo.getTargetSchema().equals(entry.getSchema())) {
            throw new AssertionError();
        }

        String sql = entry.toString();
        Set<String> set = sqlAndDatasoureMap.computeIfAbsent(sql, s -> new HashSet<>());
        set.addAll(getDatasource(targetName));
    }

    private static Set<String> getDatasource(String targetName) {
        Set<String> dataSources = new HashSet<>();
        if (ReplicaSelectorRuntime.INSTANCE.isReplicaName(targetName)) {
            ReplicaDataSourceSelector dataSourceSelector = ReplicaSelectorRuntime.INSTANCE.getDataSourceSelector(targetName);
            dataSources.addAll(dataSourceSelector.getRwaDataSourceMap().keySet());
        }
        if (ReplicaSelectorRuntime.INSTANCE.isDatasource(targetName)) {
            dataSources.add(targetName);
        }
        return dataSources;
    }
}
