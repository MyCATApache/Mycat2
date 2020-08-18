package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt3.DrdsConfig;
import io.mycat.hbt3.DrdsConst;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.*;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.util.Explains;
import io.mycat.util.Response;

import java.util.Collections;
import java.util.List;

public class DrdsRunners {

    public static void runOnDrds(MycatDataContext dataContext, Response receiver, SQLStatement  statement) {
        dataContext.block(() -> {
            if (!receiver.isExplainMode()){
                try (DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext)) {
                    DrdsConst drdsConst = new DrdsConfig();
                    DrdsRunner drdsRunners = new DrdsRunner(   drdsConst,
                            datasourceFactory,
                            PlanCache.INSTANCE,
                            dataContext);
                    Iterable<DrdsSql> drdsSqls = drdsRunners.preParse(Collections.singletonList(statement), Collections.emptyList());
                    Iterable<DrdsSql> iterable = drdsRunners.convertToMycatRel(drdsSqls);
                    DrdsSql drdsSql = iterable.iterator().next();
                    MycatContext context = new MycatContext();
                    context.params = drdsSql.getParams();
                    ExecutorImplementorImpl implementor = new ExecutorImplementorImpl(context, datasourceFactory, new TempResultSetFactoryImpl(), receiver);
                    implementor.implementRoot((MycatRel) drdsSql.getRelNode());
                } catch (Exception throwable) {
                    throw new MycatException(throwable);
                }
                return;
            }else {
                try (DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext)) {
                    DrdsConst drdsConst = new DrdsConfig();
                    DrdsRunner drdsRunners = new DrdsRunner(   drdsConst,
                            datasourceFactory,
                            PlanCache.INSTANCE,
                            dataContext);
                    Iterable<DrdsSql> drdsSqls = drdsRunners.preParse(Collections.singletonList(statement), Collections.emptyList());
                    Iterable<DrdsSql> iterable = drdsRunners.convertToMycatRel(drdsSqls);
                    DrdsSql drdsSql = iterable.iterator().next();
                    MycatContext context = new MycatContext();
                    context.params = drdsSql.getParams();
                    String mycatRelNodeText = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(drdsSql.getRelNode());
                    List<String> explain = Explains.explain(null, null, null, null, mycatRelNodeText);
                    receiver.sendExplain(null,explain);
                } catch (Exception throwable) {
                    throw new MycatException(throwable);
                }
            }
        });
    }

}