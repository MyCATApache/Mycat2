package io.mycat.sqlhandler.dml;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


import static io.mycat.sqlhandler.dml.UpdateSQLHandler.updateHandler;


public class InsertSQLHandler extends AbstractSQLHandler<MySqlInsertStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlInsertStatement> request, MycatDataContext dataContext, Response response){
        SQLExprTableSource tableSource = request.getAst().getTableSource();
        return updateHandler(request.getAst(),dataContext,tableSource,response);
    }
}
