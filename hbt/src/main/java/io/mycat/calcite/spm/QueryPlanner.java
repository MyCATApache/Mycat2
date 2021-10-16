package io.mycat.calcite.spm;

import io.mycat.DrdsSql;
import io.mycat.DrdsSqlCompiler;
import io.mycat.calcite.CodeExecuterContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

public class QueryPlanner {
    QueryPlanCache planCache2 ;

    public QueryPlanner(QueryPlanCache planCache2) {
        this.planCache2 = planCache2;
    }
    //    RelMetadataQuery relMetadataQuery = MetaClusterCurrent.wrapper(RelMetadataQuery.class);

    public RelMetadataQuery getRelMetadataQuery() {
        return RelMetadataQuery.instance();
    }

    public  CodeExecuterContext innerComputeMinCostCodeExecuterContext(DrdsSql sqlSelectStatement) {
        RelOptCluster relOptCluster = DrdsSqlCompiler.newCluster();
        List<CodeExecuterContext> codeExecuterContexts = getAcceptedMycatRelList(sqlSelectStatement);
        int size = codeExecuterContexts.size();
        switch (size) {
            case 0: {
                throw new IllegalArgumentException();
            }
            case 1: {
                return codeExecuterContexts.get(0);
            }
            default:
                class SortObject implements Comparable<SortObject> {
                    final CodeExecuterContext context;
                    final RelOptCost cost;

                    public SortObject(CodeExecuterContext context, RelOptCost cost) {
                        this.context = context;
                        this.cost = cost;
                    }

                    @Override
                    public int compareTo(@NotNull SortObject o) {
                        return (this.cost.isLt(o.cost) ? 0 : 1);
                    }
                }
                return codeExecuterContexts
                        .stream()
                        .filter(i->i!=null)
                        .map(i -> new SortObject(i, i.getMycatRel().computeSelfCost(relOptCluster.getPlanner(), getRelMetadataQuery()))).min(SortObject::compareTo)
                        .map(i -> i.context).orElse(null);
        }
    }

    public   List<CodeExecuterContext> getAcceptedMycatRelList(DrdsSql drdsSql) {
        List<CodeExecuterContext> acceptedMycatRelList = planCache2.getAcceptedMycatRelList(drdsSql);
        if (acceptedMycatRelList.isEmpty()) {
            synchronized (this){
                acceptedMycatRelList = planCache2.getAcceptedMycatRelList(drdsSql);
                if (!acceptedMycatRelList.isEmpty()){
                    return acceptedMycatRelList;
                }else {
                    PlanResultSet add = planCache2.add(false, drdsSql);
                    return Collections.singletonList(add.getContext());
                }
            }

        } else {
            return acceptedMycatRelList;
        }

//for debug,set it true
//        List<CodeExecuterContext> acceptedMycatRelList = planCache2.getAcceptedMycatRelList(drdsSql);
//        PlanResultSet add = planCache2.add(false, drdsSql);
//        return Collections.singletonList(add.getContext());
    }
}
