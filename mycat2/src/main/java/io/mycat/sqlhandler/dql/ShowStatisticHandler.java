package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.statement.SQLShowStatisticStmt;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.sql.JDBCType;

public class ShowStatisticHandler extends AbstractSQLHandler<SQLShowStatisticStmt> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowStatisticStmt> request, MycatDataContext dataContext, Response response) {
        SQLShowStatisticStmt ast = request.getAst();
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        if (!ast.isFull()) {
            resultSetBuilder.addColumnInfo("QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("RDS_QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("SLOW_QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("PHYSICAL_SLOW_QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("ERROR_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("MERGE_QUERY_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("ACTIVE_CONNECTIONS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("RT(MS)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("RDS_RT(MS)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("NET_IN(KB/S)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("NET_OUT(KB/S)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("THREAD_RUNNING", JDBCType.VARCHAR);
        }
        if (ast.isFull()) {
            resultSetBuilder.addColumnInfo("QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("RDS_QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("SLOW_QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("PHYSICAL_SLOW_QPS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("ERROR_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("VIOLATION_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("MERGE_QUERY_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("ACTIVE_CONNECTIONS", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("CONNECTION_CREATE_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("RT(MS)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("RDS_RT(MS)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("NET_IN(KB/S)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("NET_OUT(KB/S)", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("THREAD_RUNNING", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("HINT_USED_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("HINT_USED_COUNT", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("AGGREGATE_QUERY_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("AGGREGATE_QUERY_COUNT", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("TEMP_TABLE_CREATE_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("TEMP_TABLE_CREATE_COUNT", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("MULTI_DB_JOIN_PER_SECOND", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("MULTI_DB_JOIN_COUNT", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("CPU", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("FREEMEM", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("FULLGCCOUNT", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("FULLGCTIME", JDBCType.VARCHAR);
        }
        return response.sendResultSet(resultSetBuilder.build());
    }
}
