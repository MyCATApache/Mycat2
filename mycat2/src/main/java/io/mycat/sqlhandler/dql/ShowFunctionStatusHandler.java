package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class ShowFunctionStatusHandler extends AbstractSQLHandler<com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowFunctionStatusStatement> request, MycatDataContext dataContext, Response response) {
        String sql = toNormalSQL(request.getAst());
        return response.sendResultSet(runAsRowIterator(dataContext, sql));
    }


    @NotNull
    public static String toNormalSQL(MySqlShowFunctionStatusStatement requestAst) {
        SQLExpr like = requestAst.getLike();
        SQLExpr where = requestAst.getWhere();
        return generateSimpleSQL(Arrays.asList(
                new String[]{"ROUTINE_SCHEMA","Db"},
                        new String[]{"ROUTINE_NAME","Name"},
                                new String[]{"ROUTINE_TYPE","Type"},
        new String[]{"DEFINER","Definer"},
                new String[]{"LAST_ALTERED","Modified"},
                        new String[]{"CREATED","Created"},
                        new String[]{"SECURITY_TYPE","Security_type"},
                        new String[]{"ROUTINE_COMMENT","Comment"},
                        new String[]{"CHARACTER_SET_CLIENT","character_set_client"},
                        new String[]{"COLLATION_CONNECTION","collation_connection"},
                        new String[]{"DATABASE_COLLATION","`Database Collation`"}
                        ),"information_schema","ROUTINES",
                null,
                Optional.ofNullable(where).map(i->i.toString()).orElse(null),
                Optional.ofNullable(like).map(i->"SCHEMA_NAME like "+ i).orElse(null)
        ).toString();
    }
}
