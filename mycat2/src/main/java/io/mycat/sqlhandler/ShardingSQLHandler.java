package io.mycat.sqlhandler;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.*;
import io.mycat.calcite.physical.MycatCalc;
import io.mycat.util.Pair;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    private final static Logger LOGGER = LoggerFactory.getLogger(ShardingSQLHandler.class);
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response){
        HackRouter hackRouter = new HackRouter(request.getAst(), dataContext);
        try {
            if (hackRouter.analyse()) {
                Pair<String, String> plan = hackRouter.getPlan();
                return response.proxySelect(plan.getKey(),plan.getValue());
            } else {
                DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
                return drdsRunner.runOnDrds(dataContext, request.getAst(), response);
            }
        }catch (Throwable throwable){
            LOGGER.error(request.getAst().toString(),throwable);
            return Future.failedFuture(throwable);
        }

    }
}