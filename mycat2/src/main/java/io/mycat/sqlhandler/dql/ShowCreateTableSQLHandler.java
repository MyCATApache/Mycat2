package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class ShowCreateTableSQLHandler extends AbstractSQLHandler<SQLShowCreateTableStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowCreateTableStatement> request, MycatDataContext dataContext, Response response){
        SQLShowCreateTableStatement ast = request.getAst();
        SQLName name = ast.getName();
        if (name instanceof SQLIdentifierExpr){
            SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr();
            sqlPropertyExpr.setOwner(dataContext.getDefaultSchema());
            sqlPropertyExpr.setName(name.toString());
            ast.setName(sqlPropertyExpr);
        }
        return response.proxySelectToPrototype(ast.toString());
    }
}
