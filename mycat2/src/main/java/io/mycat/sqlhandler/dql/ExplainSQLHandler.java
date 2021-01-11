package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.*;
import io.mycat.DrdsRunner;
import io.mycat.DrdsSql;
import io.mycat.calcite.spm.Plan;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.HackRouter;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.mycat.util.Pair;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;


public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExplainSQLHandler.class);
    @Override
    @SneakyThrows
    protected void onExecute(SQLRequest<MySqlExplainStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        MySqlExplainStatement explainAst = request.getAst();
        if (explainAst.isDescribe()) {
            response.proxySelectToPrototype(explainAst.toString());
            return;
        }
        SQLStatement statement = request.getAst().getStatement();
        boolean forUpdate = false;
        if (statement instanceof SQLSelectStatement){
            forUpdate = ((SQLSelectStatement) explainAst .getStatement()).getSelect().getFirstQueryBlock().isForUpdate();
        }
        ResultSetBuilder builder = ResultSetBuilder.create().addColumnInfo("plan", JDBCType.VARCHAR);
        try (DataSourceFactory ignored = new DefaultDatasourceFactory(dataContext)) {
            try{
                HackRouter hackRouter = new HackRouter(statement, dataContext);
                if (hackRouter.analyse()) {
                    Pair<String, String> plan = hackRouter.getPlan();
                    builder.addObjectRowPayload(Arrays.asList("targetName: "+
                            plan.getKey()+"   sql: "+plan.getValue()));
                }else {
                    DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
                    DrdsSql drdsSql = drdsRunner.preParse(statement);
                    Plan plan = drdsRunner.getPlan(dataContext, drdsSql);
                    List<String> explain = plan.explain(dataContext,drdsSql);
                    for (String s1 : explain) {
                        builder.addObjectRowPayload(Arrays.asList(s1));
                    }
                }

            }catch (Throwable th){
                LOGGER.error("",th);
                builder.addObjectRowPayload(Arrays.asList(th.toString()));
            }
            response.sendResultSet(RowIterable.create(builder.build()));
        }
    }
}
