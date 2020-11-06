package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class RenameTableSQLHandler extends AbstractSQLHandler<MySqlRenameTableStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlRenameTableStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.sendOk();
    }
}
