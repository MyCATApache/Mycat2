package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCheckTableStatement;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.table.GlobalTableHandler;
import io.mycat.calcite.table.NormalTableHandler;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.router.ShardingTableHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.sql.JDBCType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MySQLCheckHandler extends AbstractSQLHandler<MySqlCheckTableStatement> {

    @AllArgsConstructor
    @Getter
    static class Each {
        final DataNode dataNode;
        final List<Map<String, Object>> info;
    }

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlCheckTableStatement> request, MycatDataContext dataContext, Response response) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("TABLE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("OP", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("MSG_TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("MSG_TEXT", JDBCType.VARCHAR);

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);

        List<Throwable> throwables = Collections.synchronizedList(new LinkedList<>());
        MySqlCheckTableStatement ast = request.getAst();
        for (SQLExprTableSource table : ast.getTables()) {
            resolveSQLExprTableSource(table, dataContext);
            String schemaName = SQLUtils.normalize(table.getSchema());
            String tableName = SQLUtils.normalize(table.getTableName());
            TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
            List<Map<String, Object>> prototypeColumnInfo = null;
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
                prototypeColumnInfo = JdbcUtils.executeQuery(connection.getRawConnection(),
                        "show full columns from  " + table, Collections.emptyList());
            } catch (Throwable throwable) {
                throwables.add(throwable);
            }
            List<Map<String, Object>> curRrototypeColumnInfo = prototypeColumnInfo;
            Set<String> errorInfo = new HashSet<>();
            switch (tableHandler.getType()) {
                case SHARDING: {
                    ShardingTableHandler shardingTableHandler = (ShardingTableHandler) tableHandler;
                    errorInfo = check(metadataManager, jdbcConnectionManager, throwables, curRrototypeColumnInfo,
                            shardingTableHandler.dataNodes().parallelStream());
                    break;
                }
                case GLOBAL: {
                    GlobalTableHandler globalTableHandler = (GlobalTableHandler) tableHandler;
                    errorInfo = check(metadataManager, jdbcConnectionManager, throwables, curRrototypeColumnInfo,
                            globalTableHandler.getGlobalDataNode().parallelStream());
                    break;
                }
                case NORMAL: {
                    break;
                }
                case CUSTOM: {
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + tableHandler.getType());
            }

            resultSetBuilder.addObjectRowPayload(Arrays.asList(table.toString(), "check", errorInfo.isEmpty() ? "Ok" : "Error", String.join(",", errorInfo)));
        }
        return response.sendResultSet(resultSetBuilder.build());
    }

    @NotNull
    private Set<String> check(MetadataManager metadataManager, JdbcConnectionManager jdbcConnectionManager, List<Throwable> throwables, List<Map<String, Object>> curRrototypeColumnInfo, Stream<DataNode> dataNodeStream) {
        Set<String> errorInfo;
        List<Each> eachColumnInfos = Collections.synchronizedList(new LinkedList<>());
        dataNodeStream.forEach(dataNode -> {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(dataNode.getTargetName())) {
                eachColumnInfos.add(new Each(dataNode, JdbcUtils.executeQuery(connection.getRawConnection(),
                        "show full columns from  " + dataNode.getTargetSchemaTable(), Collections.emptyList())));
            } catch (Throwable throwable) {
                throwables.add(throwable);
            }
        });
        errorInfo = eachColumnInfos.stream()
                .filter(i -> !i.getInfo().equals(curRrototypeColumnInfo))
                .map(i -> i.getDataNode().getUniqueName()).collect(Collectors.toSet());
        return errorInfo;
    }
}
