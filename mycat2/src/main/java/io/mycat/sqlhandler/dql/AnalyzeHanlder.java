package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.MetadataManager;
import io.mycat.TableHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.statistic.StatisticCenter;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author Junwen Chen
 **/
public class AnalyzeHanlder extends AbstractSQLHandler<MySqlAnalyzeStatement> {
    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<MySqlAnalyzeStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        MySqlAnalyzeStatement ast = request.getAst();
        List<SQLExprTableSource> tableSources = Optional.ofNullable(ast.getTableSources()).orElse(Collections.emptyList());
        if (tableSources.isEmpty()) {
            return response.sendError(new MycatException("need tables"));
        } else {
            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
            resultSetBuilder.addColumnInfo("Table", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("Op", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("Msg_type", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("Msg_Text", JDBCType.VARCHAR);

            for (SQLExprTableSource tableSource : tableSources) {
                String schemaName = SQLUtils.normalize(tableSource.getSchema());
                String tableName = SQLUtils.normalize(tableSource.getTableName());
                resultSetBuilder.addObjectRowPayload(Arrays.asList(
                        schemaName+"."+tableName,
                        "analyze",
                        "status",
                        "OK"
                ));
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
                if (tableHandler == null) {
                    return response.sendError(new MycatException(tableSource + "不存在"));
                }
                StatisticCenter.INSTANCE.computeTableRowCount(tableHandler);
            }
            return response.sendResultSet(resultSetBuilder.build());
        }
    }
}