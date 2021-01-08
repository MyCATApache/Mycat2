//package io.mycat.sqlhandler;
//
//import com.alibaba.druid.sql.ast.SQLStatement;
//import io.mycat.*;
//import io.mycat.calcite.MycatRel;
//import io.mycat.calcite.physical.MycatInsertRel;
//import io.mycat.calcite.physical.MycatUpdateRel;
//import io.mycat.calcite.plan.PlanImplementor;
//import io.mycat.calcite.rewriter.OptimizationContext;
//import io.mycat.calcite.spm.Plan;
//import io.mycat.calcite.spm.PlanCache;
//import io.mycat.calcite.spm.PlanImpl;
//import lombok.SneakyThrows;
//import org.apache.calcite.runtime.CodeExecuterContext;
//
//import static io.mycat.DrdsRunner.getCodeExecuterContext;
//
//public class DrdsRunners {
//    public static void main(String[] args) {
//
//    }
//
//    @SneakyThrows
//    public void runOnDrds(MycatDataContext dataContext,
//                          SQLStatement statement,
//                          PlanImplementor planImplementor) {
//        DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
//        DrdsSql drdsSql = drdsRunner.preParse(statement);
//        PlanCache planCache = drdsRunner.getPlanCache();
//        OptimizationContext optimizationContext = new OptimizationContext();
//        Plan plan = drdsRunner.convertToExecuter(drdsSql, dataContext, optimizationContext);
//        planCache.put(drdsSql.getParameterizedString(), plan);
//        switch (plan.getType()) {
//            case LOGICAL:
//                assert false;
//            case PHYSICAL:
//                planImplementor.execute(plan);
//                break;
//            case UPDATE:
//                planImplementor.execute((MycatUpdateRel) plan.getLogical());
//                break;
//            case INSERT:
//                planImplementor.execute((MycatInsertRel) plan.getLogical());
//                break;
//        }
//    }
//
//
//    public void runHbtOnDrds(MycatDataContext dataContext, String statement, PlanImplementor planImplementor) throws Exception {
//        DrdsConst drdsConst = new DrdsConfig();
//        DrdsRunner drdsRunners = new DrdsRunner(drdsConst,
//                PlanCache.INSTANCE);
//        MycatRel mycatRel = drdsRunners.doHbt(statement, dataContext);
//        CodeExecuterContext codeExecuterContext = getCodeExecuterContext(mycatRel);
//        planImplementor.execute(new PlanImpl(mycatRel, codeExecuterContext));
//    }
//
//}