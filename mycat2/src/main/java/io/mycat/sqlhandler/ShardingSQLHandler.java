package io.mycat.sqlhandler;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.*;
import io.mycat.util.Pair;
import io.vertx.core.Future;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response){
        HackRouter hackRouter = new HackRouter(request.getAst(), dataContext);
        if (false) {
            Pair<String, String> plan = hackRouter.getPlan();
            return response.proxySelect(plan.getKey(),plan.getValue());
        } else {
            DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
            return drdsRunner.runOnDrds(dataContext, request.getAst(), response);
        }
    }
}