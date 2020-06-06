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
import io.mycat.metadata.ShardingTableHandler;
import io.mycat.metadata.TableHandler;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
            if (tableHandler instanceof ShardingTableHandler) {
                ShardingTableHandler handler = (ShardingTableHandler) tableHandler;
                for (BackendTableInfo shardingBackend : handler.getShardingBackends()) {
                    makeTask(ast, tableName, sqlAndDatasoureMap, shardingBackend);
                }
            } else if (tableHandler instanceof GlobalTableHandler) {
                GlobalTableHandler handler = (GlobalTableHandler) tableHandler;
                for (BackendTableInfo shardingBackend : handler.getDataNodeMap().values()) {
                    makeTask(ast, tableName, sqlAndDatasoureMap, shardingBackend);
                }
            } else {
                throw new UnsupportedOperationException("UnsupportedOperation :" + tableHandler);
            }
            ExecutorService fetchDataExecutorService = JdbcRuntime.INSTANCE.getFetchDataExecutorService();
            List<CompletableFuture> resList = new ArrayList<>();
            sqlAndDatasoureMap.forEach((sql, dataSources) -> {
                for (String dataSource : dataSources) {
                    resList.add(CompletableFuture.runAsync(() -> {
                        try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(dataSource)) {
                            connection.executeUpdate(sql, false, 0);
                        }
                    }, fetchDataExecutorService));
                }
            });

            List<Throwable> throwables = new ArrayList<>();
            CompletableFuture.allOf(resList.toArray(new CompletableFuture[0]))
                    .exceptionally(throwable -> {
                        LOGGER.error("执行SQL失败", throwable);
                        throwables.add(throwable);
                        return null;
                    })
                    .get(5, TimeUnit.MINUTES);

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


    private void makeTask(SQLCreateTableStatement ast, String tableName, Map<String, Set<String>> sqlAndDatasoureMap, BackendTableInfo shardingBackend) {
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

        Set<String> dataSources = new HashSet<>();
        if (ReplicaSelectorRuntime.INSTANCE.isReplicaName(targetName)) {
            ReplicaDataSourceSelector dataSourceSelector = ReplicaSelectorRuntime.INSTANCE.getDataSourceSelector(targetName);
            dataSources.addAll(dataSourceSelector.getRwaDataSourceMap().keySet());
        }
        if (ReplicaSelectorRuntime.INSTANCE.isDatasource(targetName)) {
            dataSources.add(tableName);
        }

        String sql = entry.toString();
        Set<String> set = sqlAndDatasoureMap.computeIfAbsent(sql, s -> new HashSet<>());
        set.addAll(dataSources);
    }
}
