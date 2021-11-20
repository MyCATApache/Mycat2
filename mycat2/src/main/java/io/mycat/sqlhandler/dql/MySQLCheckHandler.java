/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCheckTableStatement;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.table.GlobalTableHandler;
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
        final Partition partition;
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
            Set<String> errorInfo = new HashSet<>();
            switch (tableHandler.getType()) {
                case SHARDING: {
                    ShardingTableHandler shardingTableHandler = (ShardingTableHandler) tableHandler;
                    errorInfo = check(metadataManager, jdbcConnectionManager, throwables,
                            shardingTableHandler.dataNodes().parallelStream());
                    break;
                }
                case GLOBAL: {
                    GlobalTableHandler globalTableHandler = (GlobalTableHandler) tableHandler;
                    errorInfo = check(metadataManager, jdbcConnectionManager, throwables,
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
    private Set<String> check(MetadataManager metadataManager, JdbcConnectionManager jdbcConnectionManager, List<Throwable> throwables,Stream<Partition> dataNodeStream) {
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
        if (eachColumnInfos.isEmpty()||eachColumnInfos.size() == 1){
            return Collections.emptySet();
        }
        List<Map<String, Object>> curRrototypeColumnInfo = eachColumnInfos.get(0).getInfo();
        errorInfo = eachColumnInfos.stream()
                .filter(i -> !i.getInfo().equals(curRrototypeColumnInfo))
                .map(i -> i.getPartition().getUniqueName()).collect(Collectors.toSet());
        return errorInfo;
    }
}
