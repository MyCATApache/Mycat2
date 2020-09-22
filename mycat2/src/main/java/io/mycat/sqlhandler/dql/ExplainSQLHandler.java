package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatdbCommand;
import io.mycat.hbt3.DrdsConfig;
import io.mycat.hbt3.DrdsConst;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.DatasourceFactory;
import io.mycat.hbt4.DefaultDatasourceFactory;
import io.mycat.hbt4.MycatRel;
import io.mycat.hbt4.PlanCache;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Explains;
import io.mycat.util.Response;
import io.vertx.core.spi.resolver.ResolverProvider;
import lombok.SneakyThrows;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {


    @Override
    @SneakyThrows
    protected void onExecute(SQLRequest<MySqlExplainStatement> request, MycatDataContext dataContext, Response response) {
        MySqlExplainStatement ast = request.getAst();
        if(ast.isDescribe()){
            response.tryBroadcastShow(ast.toString());
            return ;
        }
        try (DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext)) {
            DrdsConst drdsConst = new DrdsConfig();
            DrdsRunner drdsRunners = new DrdsRunner(drdsConst,
                    datasourceFactory,
                    PlanCache.INSTANCE,
                    dataContext);
            Iterable<DrdsSql> drdsSqls = drdsRunners.preParse(Collections.singletonList(ast.getStatement()), Collections.emptyList());
            Iterable<DrdsSql> iterable = drdsRunners.convertToMycatRel(drdsSqls);
            DrdsSql drdsSql = iterable.iterator().next();
            MycatRel relNode = (MycatRel) drdsSql.getRelNode();
            String s = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(relNode);
            ResultSetBuilder builder = ResultSetBuilder.create().addColumnInfo("plan", JDBCType.VARCHAR);
            List<String> explain = Explains.explain(ast.getStatement().toString(), null, null, null, s);
            for (String s1 : explain) {
                builder.addObjectRowPayload(Arrays.asList(s1));
            }
            response.sendResultSet(  RowIterable.create(builder.build()));
        }
    }
}
