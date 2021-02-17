package io.mycat.sqlhandler;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;
import io.vertx.core.impl.future.PromiseInternal;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) throws Exception {
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