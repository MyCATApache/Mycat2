package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ShowTriggersSQLHandler extends AbstractSQLHandler<MySqlShowTriggersStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowTriggersStatement> request, MycatDataContext dataContext, Response response) {
        MySqlShowTriggersStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
         String database = ast.getDatabase().getSimpleName();
         SQLExpr like = ast.getLike();
         SQLExpr where = ast.getWhere();
        if (database == null) {
            return response.sendError(new MycatException("NO DATABASES SELECTED"));
        }
        String sql = toNormalSQL(request.getAst());
        return response.sendResultSet(runAsRowIterator(dataContext, sql));
    }

    private String toNormalSQL(MySqlShowTriggersStatement ast) {
        String database = SQLUtils.normalize(ast.getDatabase().getSimpleName());
        SQLExpr like = ast.getLike();
        SQLExpr where = ast.getWhere();

        List<String[]> strings = Arrays.asList(
                new String[]{"TRIGGER_NAME","Trigger"},
                new String[]{"EVENT_MANIPULATION","Event"},
                new String[]{"EVENT_OBJECT_TABLE","Table"},
                new String[]{"ACTION_STATEMENT","Statement"},
                new String[]{"ACTION_TIMING","Timing"},
                new String[]{"CREATED","Created"},
                new String[]{"SQL_MODE","sql_mode"},
                new String[]{"DEFINER","Definer"},
                new String[]{"CHARACTER_SET_CLIENT","character_set_client"},
                new String[]{"COLLATION_CONNECTION","collation_connection"},
                new String[]{"DATABASE_COLLATION","`Database Collation`"}
        );
        return generateSimpleSQL(strings, "INFORMATION_SCHEMA", "TRIGGERS", "TRIGGER_NAME = '" +database+"'",
                Optional.ofNullable(where).map(i -> i.toString()).orElse(null)
                ,
                Optional.ofNullable(like).map(i -> "TRIGGER_NAME like " + i.toString()).orElse(null)
        ).toString();
    }
}
