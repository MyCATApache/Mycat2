package io.mycat.sqlHandler.dml;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;





public class LoadDataInFileSQLHandler extends AbstractSQLHandler<MySqlLoadDataInFileStatement> {



    public void init(){

    }

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlLoadDataInFileStatement> request, MycatDataContext dataContext, Response response) {
        return ExecuteCode.NOT_PERFORMED;
    }
}
