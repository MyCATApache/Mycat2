package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropFunctionStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.Collections;

public class SQLDropFunctionHandler extends AbstractSQLHandler<SQLDropFunctionStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropFunctionStatement> request, MycatDataContext dataContext, Response response) {
        SQLDropFunctionStatement ast = request.getAst();
        if(ast.getName() instanceof SQLIdentifierExpr){
            String defaultSchema = dataContext.getDefaultSchema();
            if (defaultSchema!=null){
                ast.setName(new SQLPropertyExpr("`"+defaultSchema+"`",((SQLIdentifierExpr) ast.getName()).getName()));
            }
        }
        return response.proxyUpdateToPrototype(ast.toUnformattedString(), Collections.emptyList());
    }
}
