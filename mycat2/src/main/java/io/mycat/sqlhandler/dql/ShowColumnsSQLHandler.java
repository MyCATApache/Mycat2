package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


/**
 * @ chenjunwen
 */

public class ShowColumnsSQLHandler extends AbstractSQLHandler<SQLShowColumnsStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowColumnsStatement> request, MycatDataContext dataContext, Response response){
        SQLShowColumnsStatement ast = request.getAst();
        return response.proxySelectToPrototype(ast.toString());
    }
}
