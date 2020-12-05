package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.*;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Explains;
import io.mycat.util.Response;
import lombok.SneakyThrows;
import org.apache.calcite.rel.core.Collect;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {

    @Override
    @SneakyThrows
    protected void onExecute(SQLRequest<MySqlExplainStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        MySqlExplainStatement ast = request.getAst();
        if (ast.isDescribe()) {
            response.tryBroadcastShow(ast.toString());
            return;
        }
        try (DataSourceFactory ignored = new DefaultDatasourceFactory(dataContext)) {
            DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
            Iterable<DrdsSql> drdsSqls = drdsRunner.preParse(Collections.singletonList(ast.getStatement()), Collections.emptyList());
            Iterable<DrdsSql> iterable = drdsRunner.convertToMycatRel(drdsSqls, dataContext);
            DrdsSql drdsSql = iterable.iterator().next();
            MycatRel relNode = (MycatRel) drdsSql.getRelNode();
            String s = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(relNode);
            ResultSetBuilder builder = ResultSetBuilder.create().addColumnInfo("plan", JDBCType.VARCHAR);
            List<String> explain = Explains.explain(ast.getStatement().toString(), null, null, null, s);
            for (String s1 : explain) {
                builder.addObjectRowPayload(Arrays.asList(s1));
            }
            ResponseExecutorImplementor responseExecutorImplementor =
                    new ResponseExecutorImplementor(ignored, new TempResultSetFactoryImpl(), response);
            responseExecutorImplementor.setParams(drdsSql.getParams());
            Executor executor = relNode.implement(responseExecutorImplementor);
            ExplainWriter explainWriter = new ExplainWriter();
            executor.explain(explainWriter);
            String[] split = explainWriter.getText().toString().split("\n");
            for (String s1 : split) {
                builder.addObjectRowPayload(Arrays.asList(s1));
            }
            response.sendResultSet(RowIterable.create(builder.build()));
        }
    }
}
