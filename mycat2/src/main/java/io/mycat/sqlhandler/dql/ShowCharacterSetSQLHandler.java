package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class ShowCharacterSetSQLHandler extends AbstractSQLHandler<MySqlShowCharacterSetStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowCharacterSetStatement> request, MycatDataContext dataContext, Response response) {
        return response.proxySelectToPrototype(request.getAst().toString());
    }
}
