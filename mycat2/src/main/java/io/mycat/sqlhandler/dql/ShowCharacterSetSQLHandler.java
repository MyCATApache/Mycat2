package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class ShowCharacterSetSQLHandler extends AbstractSQLHandler<MySqlShowCharacterSetStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<MySqlShowCharacterSetStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        return response.proxySelectToPrototype(request.getAst().toString());
    }
}
