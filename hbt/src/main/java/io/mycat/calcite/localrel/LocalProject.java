package io.mycat.calcite.localrel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.List;

public class LocalProject extends Project implements LocalRel{
    protected LocalProject(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, hints, input, projects, rowType);
    }

    @Override
    public LocalProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new LocalProject(getCluster(),traitSet,getHints(),input,projects,rowType);
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.9);
    }

    static final RelFactories.ProjectFactory PROJECT_FACTORY =
            (input, hints, projects, fieldNames) -> {
                final RelOptCluster cluster = input.getCluster();
                final RelDataType rowType =
                        RexUtil.createStructType(cluster.getTypeFactory(), projects,
                                fieldNames, SqlValidatorUtil.F_SUGGESTER);
                return new LocalProject(cluster, input.getTraitSet(),hints, input, projects,
                        rowType);
            };

}
