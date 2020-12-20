package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.hbt3.DrdsConfig;
import io.mycat.hbt3.DrdsConst;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.*;
import io.mycat.sqlrecorder.SqlRecord;
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