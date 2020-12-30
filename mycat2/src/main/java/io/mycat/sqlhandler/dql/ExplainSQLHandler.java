package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.*;
import io.mycat.DrdsRunner;
import io.mycat.DrdsSql;
import io.mycat.calcite.executor.TempResultSetFactoryImpl;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Explains;
import io.mycat.Response;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExplainSQLHandler.class);
    @Override
    @SneakyThrows
    protected void onExecute(SQLRequest<MySqlExplainStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        MySqlExplainStatement ast = request.getAst();
        if (ast.isDescribe()) {
            response.proxySelectToPrototype(ast.toString());
            return;
        }
        boolean forUpdate = false;
        if (ast .getStatement() instanceof SQLSelectStatement){
            forUpdate = ((SQLSelectStatement) ast .getStatement()).getSelect().getFirstQueryBlock().isForUpdate();
        }
        ResultSetBuilder builder = ResultSetBuilder.create().addColumnInfo("plan", JDBCType.VARCHAR);
        try (DataSourceFactory ignored = new DefaultDatasourceFactory(dataContext)) {
            try{
                DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
                Iterable<DrdsSql> drdsSqls = drdsRunner.preParse(Collections.singletonList(ast.getStatement()), Collections.emptyList());
                Iterable<DrdsSql> iterable = drdsRunner.convertToMycatRel(drdsSqls, dataContext);
                DrdsSql drdsSql = iterable.iterator().next();
                MycatRel relNode = (MycatRel) drdsSql.getRelNode();
                String s = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(relNode);
                List<String> explain = Explains.explain(ast.getStatement().toString(), null, null, null, s);
                for (String s1 : explain) {
                    builder.addObjectRowPayload(Arrays.asList(s1));
                }

                ResponseExecutorImplementor responseExecutorImplementor =
                        new ResponseExecutorImplementor(dataContext,ignored, new TempResultSetFactoryImpl(), response);
                responseExecutorImplementor.setParams(drdsSql.getParams());
                responseExecutorImplementor.setForUpdate(forUpdate);
                Executor executor = relNode.implement(responseExecutorImplementor);
                ExplainWriter explainWriter = new ExplainWriter();
                executor.explain(explainWriter);
                String[] split = explainWriter.getText().toString().split("\n");
                for (String s1 : split) {
                    builder.addObjectRowPayload(Arrays.asList(s1));
                }
            }catch (Throwable th){
                LOGGER.error("",th);
                builder.addObjectRowPayload(Arrays.asList(th.toString()));
            }
            response.sendResultSet(RowIterable.create(builder.build()));
        }
    }
}
