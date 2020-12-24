package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;





public class LoadDataInFileSQLHandler extends AbstractSQLHandler<MySqlLoadDataInFileStatement> {



    public void init(){

    }

    @Override
    protected void onExecute(SQLRequest<MySqlLoadDataInFileStatement> request, MycatDataContext dataContext, Response response) throws Exception {

    }
}
