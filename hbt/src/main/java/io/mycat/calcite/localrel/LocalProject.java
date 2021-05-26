package io.mycat.calcite.localrel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.List;
import java.util.Objects;

public class LocalProject extends Project implements LocalRel{
    protected LocalProject(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits.replace(LocalConvention.INSTANCE), hints, input, projects, rowType);
    }

    public LocalProject(RelInput relInput) {
        this(relInput.getCluster(),
                relInput.getTraitSet(),
                ImmutableList.of(),
                relInput.getInput(),
                Objects.requireNonNull(relInput.getExpressionList("exprs")),
                Objects.requireNonNull(relInput.getRowType("exprs", "fields")));
    }
    public static LocalProject create(Project logicalProject, RelNode input) {
        return new LocalProject(logicalProject.getCluster(), logicalProject.getTraitSet(), logicalProject.getHints(), input,logicalProject.getProjects(),logicalProject.getRowType());
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
