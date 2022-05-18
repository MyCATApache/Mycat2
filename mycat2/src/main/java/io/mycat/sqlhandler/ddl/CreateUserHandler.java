package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import lombok.SneakyThrows;

public class CreateUserHandler extends AbstractSQLHandler<MySqlCreateUserStatement> {
    @Override
    @SneakyThrows
    protected Future<Void> onExecute(SQLRequest<MySqlCreateUserStatement> request, MycatDataContext dataContext, Response response) {
        return response.sendOk();
    }

}
