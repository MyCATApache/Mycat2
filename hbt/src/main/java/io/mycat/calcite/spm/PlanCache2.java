package io.mycat.calcite.spm;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.rewriter.OptimizationContext;
import lombok.SneakyThrows;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PlanCache2 implements QueryPlanCache {
    private PlanIds planIds = new PlanIds();
    private PlanManagerPersistor persistor;
    private ConcurrentHashMap<Constraint, Baseline> map = new ConcurrentHashMap<>();
    private final static Logger log = LoggerFactory.getLogger(PlanCache2.class);

    public PlanCache2(PlanManagerPersistor persistor) {
        this.persistor = persistor;
    }

    public void init() {
        this.map.putAll(persistor.loadAllBaseline());
        this.map.values().stream().flatMap(c -> {
            return c.getPlanList().stream();
        }).forEach(p ->getCodeExecuterContext(p));
    }

    public void delete(List<String> uniqueTables) {
        persistor.deleteBaselineByExtraConstraint(uniqueTables);
    }

    public Baseline getBaseline(DrdsSql baseLineSql) {
        Constraint constraint = baseLineSql.constraint();
        return map.computeIfAbsent(constraint, s -> persistor.loadBaselineByBaseLineSql(baseLineSql.getParameterizedSql(), constraint)
                .orElseGet(() -> {
                    SQLStatement parameterizedStatement = baseLineSql.getParameterizedStatement();
                    List<String> uniqueNames = new LinkedList<>();
                    MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                    parameterizedStatement.accept(new MySqlASTVisitorAdapter() {
                        @Override
                        public boolean visit(SQLExprTableSource x) {
                            String tableName = x.getTableName();
                            if (tableName != null) {
                                String schema = x.getSchema();
                                TableHandler table = metadataManager.getTable(SQLUtils.normalize(schema), SQLUtils.normalize(tableName));
                                uniqueNames.add(table.getUniqueName());
                            }
                            return super.visit(x);
                        }
                    });
                    Baseline baseline = new Baseline(planIds.nextBaselineId(), baseLineSql.getParameterizedSql(), constraint, null,
                            new ExtraConstraint(uniqueNames));
                    return baseline;
                }));
    }

    public PlanResultSet saveBaselinePlan(boolean fix, boolean persist, Baseline baseline, BaselinePlan newBaselinePlan) {
        Objects.requireNonNull(newBaselinePlan.getAttach());
        Set<BaselinePlan> planList = baseline.getPlanList();
        for (BaselinePlan plan : planList) {
            if (plan.getSql().equals(newBaselinePlan.getSql())) {
                if (plan.getRel().equals(newBaselinePlan.getRel())) {
                    newBaselinePlan = plan;
                }
            }
        }
        CodeExecuterContext codeExecuterContext = getCodeExecuterContext(newBaselinePlan);
        if (persist) {
            BaselinePlan newBaselinePlan1 = newBaselinePlan;
            persistor.savePlan(newBaselinePlan1, fix);
        }
        baseline.getPlanList().add(newBaselinePlan);
        map.put(baseline.getConstraint(), baseline);
        return new PlanResultSet(newBaselinePlan.getBaselineId(), true, codeExecuterContext);
    }

    @SneakyThrows
    public static CodeExecuterContext getCodeExecuterContext(BaselinePlan plan) {
        boolean forUpdate = DrdsSql.isForUpdate(plan.getSql());
        Object attach = plan.getAttach();
        if (attach != null) {
            return (CodeExecuterContext) attach;
        }
        String rel = plan.getRel();

        synchronized (plan) {
            try {
                DrdsSqlCompiler drdsSqlCompiler = MetaClusterCurrent.wrapper(DrdsSqlCompiler.class);
                RelJsonReader relJsonReader = new RelJsonReader(DrdsSqlCompiler.newCluster(), drdsSqlCompiler.newCalciteCatalogReader(), null);
                MycatRel mycatRel = (MycatRel) relJsonReader.read(rel);
                CodeExecuterContext codeExecuterContext = DrdsExecutorCompiler.getCodeExecuterContext(mycatRel, forUpdate);
                plan.setAttach(codeExecuterContext);
            } catch (Throwable throwable) {
                log.error("", throwable);
            }
        }

        return (CodeExecuterContext) plan.getAttach();
    }


    public List<CodeExecuterContext> getAcceptedMycatRelList(DrdsSql baselineSql) {
        Baseline baseline = getBaseline(baselineSql);
        List<CodeExecuterContext> list = new ArrayList<>(1);
        for (BaselinePlan p : baseline.getPlanList()) {
            if (p.isAccept()) {
                CodeExecuterContext codeExecuterContext = getCodeExecuterContext(p);
                list.add(codeExecuterContext);
            }
        }
        return list;
    }

    public synchronized PlanResultSet add(boolean fix, DrdsSql drdsSql) {
        Long baselineId = null;
        try {
            Baseline baseline = this.getBaseline(drdsSql);
            DrdsSqlCompiler drdsSqlCompiler = MetaClusterCurrent.wrapper(DrdsSqlCompiler.class);
            OptimizationContext optimizationContext = new OptimizationContext();
            MycatRel mycatRel = drdsSqlCompiler.dispatch(optimizationContext, drdsSql);
            RelJsonWriter relJsonWriter = new RelJsonWriter();
            mycatRel.explain(relJsonWriter);
            BaselinePlan newBaselinePlan = new BaselinePlan(drdsSql.getParameterizedSql(), relJsonWriter.asString(), planIds.nextBaselineId(), baselineId = baseline.getBaselineId(), null);
            getCodeExecuterContext(newBaselinePlan);
            return saveBaselinePlan(fix, false, baseline, newBaselinePlan);
        } catch (Throwable throwable) {
            log.error("", throwable);
            return new PlanResultSet(baselineId, false, null);
        }
    }

    public void clearCache(){
        map.clear();
    }

}
