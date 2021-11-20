package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

public class ShowCreateDatabaseHandler extends AbstractSQLHandler<MySqlShowCreateDatabaseStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowCreateDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        MySqlShowCreateDatabaseStatement ast = request.getAst();
        SQLExpr database = ast.getDatabase();
        return response.proxySelectToPrototype(ast.toString());
    }
}
