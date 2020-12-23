package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.calcite.ExecutorImplementor;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.spm.PlanCache;
import io.mycat.DrdsConfig;
import io.mycat.DrdsConst;
import io.mycat.DrdsRunner;
import io.mycat.DrdsSql;
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
        DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
        Iterable<DrdsSql> drdsSqls = drdsRunner.preParse(Collections.singletonList(statement), Collections.emptyList());
        Iterable<DrdsSql> iterable = drdsRunner.convertToMycatRel(drdsSqls, dataContext);
        DrdsSql drdsSql = iterable.iterator().next();
        executorImplementor.setParams(drdsSql.getParams());
        executorImplementor.implementRoot((MycatRel) drdsSql.getRelNode(), drdsSql.getAliasList());
    }

    public static void runHbtOnDrds(MycatDataContext dataContext, String statement, ExecutorImplementor executorImplementor) throws Exception {
        MycatContext.CONTEXT.set(dataContext);
        DrdsConst drdsConst = new DrdsConfig();
        DrdsRunner drdsRunners = new DrdsRunner(drdsConst,
                PlanCache.INSTANCE);
        MycatRel mycatRel = drdsRunners.doHbt(statement, dataContext);
        executorImplementor.implementRoot(mycatRel, Collections.emptyList());
    }
}