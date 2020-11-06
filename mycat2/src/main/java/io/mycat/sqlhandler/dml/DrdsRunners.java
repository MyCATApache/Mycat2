package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.MycatDataContext;
import io.mycat.hbt3.DrdsConfig;
import io.mycat.hbt3.DrdsConst;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.*;
import lombok.SneakyThrows;
import org.apache.calcite.MycatContext;

import java.util.Collections;

public class DrdsRunners {
    public static void main(String[] args) {

    }

    @SneakyThrows
    public static void runOnDrds(MycatDataContext dataContext,
                                 SQLStatement statement,
                                 ExecutorImplementor executorImplementor) {
        MycatContext.CONTEXT.set(dataContext);
        try (DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext)) {
            DrdsConst drdsConst = new DrdsConfig();
            DrdsRunner drdsRunners = new DrdsRunner(drdsConst,
                    datasourceFactory,
                    PlanCache.INSTANCE,
                    dataContext);
            Iterable<DrdsSql> drdsSqls = drdsRunners.preParse(Collections.singletonList(statement), Collections.emptyList());
            Iterable<DrdsSql> iterable = drdsRunners.convertToMycatRel(drdsSqls);
            DrdsSql drdsSql = iterable.iterator().next();
            executorImplementor.setParams(drdsSql.getParams());
            executorImplementor.implementRoot((MycatRel) drdsSql.getRelNode());
        } finally {

        }
    }

    public static void runHbtOnDrds(MycatDataContext dataContext, String statement, ExecutorImplementor executorImplementor) throws Exception {
        MycatContext.CONTEXT.set(dataContext);
        try (DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext)) {
            DrdsConst drdsConst = new DrdsConfig();
            DrdsRunner drdsRunners = new DrdsRunner(drdsConst,
                    datasourceFactory,
                    PlanCache.INSTANCE,
                    dataContext);
            MycatRel mycatRel = drdsRunners.doHbt(statement);
            executorImplementor.implementRoot(mycatRel);
        } finally {

        }
    }
}