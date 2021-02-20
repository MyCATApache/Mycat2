package io.mycat.sqlhandler.dml;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class LoadDataInFileSQLHandler extends AbstractSQLHandler<MySqlLoadDataInFileStatement> {



    public void init(){

    }

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlLoadDataInFileStatement> request, MycatDataContext dataContext, Response response) {
        return response.sendOk();
    }
}
