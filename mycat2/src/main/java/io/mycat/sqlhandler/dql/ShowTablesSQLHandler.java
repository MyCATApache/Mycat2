package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ShowTablesSQLHandler extends AbstractSQLHandler<SQLShowTablesStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowTablesSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowTablesStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowTablesStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
        SQLName database = ast.getDatabase();
        if (database == null) {
           return response.sendError(new MycatException("NO DATABASES SELECTED"));
        }
     return response.proxySelectToPrototype(ast.toString());
    }


}
