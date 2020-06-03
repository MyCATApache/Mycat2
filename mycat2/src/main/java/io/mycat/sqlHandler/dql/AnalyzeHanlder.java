package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.TableHandler;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.statistic.StatisticCenter;
import io.mycat.util.Response;

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
    protected ExecuteCode onExecute(SQLRequest<MySqlAnalyzeStatement> request, MycatDataContext dataContext, Response response) {
        MySqlAnalyzeStatement ast = request.getAst();
        List<SQLExprTableSource> tableSources = Optional.ofNullable(ast.getTableSources()).orElse(Collections.emptyList());
        if (tableSources.isEmpty()) {
            response.sendError(new MycatException("need tables"));
            return ExecuteCode.PERFORMED;
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
                TableHandler tableHandler = MetadataManager.INSTANCE.getTable(schemaName, tableName);
                if (tableHandler == null) {
                    response.sendError(new MycatException(tableSource + "不存在"));
                    return ExecuteCode.PERFORMED;
                }
                StatisticCenter.INSTANCE.computeTableRowCount(tableHandler);
            }
            response.sendResultSet(resultSetBuilder.build(),null);
            return ExecuteCode.PERFORMED;
        }


    }
}