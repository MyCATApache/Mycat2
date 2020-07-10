package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatProject;
import io.mycat.hbt4.physical.rules.CheckingUserDefinedFunctionVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Project} to
 * an {@link MycatProjectRule}.
 */
public class MycatProjectRule extends MycatConverterRule {

    /**
     * Creates a MycatProjectRule.
     */
    public MycatProjectRule(final MycatConvention out,
                            RelBuilderFactory relBuilderFactory) {
        super(Project.class, (Predicate<Project>) project ->
                        true,
                MycatRules.convention, out, relBuilderFactory, "MycatProjectRule");
    }

    private static boolean userDefinedFunctionInProject(Project project) {
        CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
        for (RexNode node : project.getChildExps()) {
            node.accept(visitor);
            if (visitor.containsUserDefinedFunction()) {
                return true;
            }
        }
        return false;
    }

    public RelNode convert(RelNode rel) {
        final Project project = (Project) rel;

        return new MycatProject(
                rel.getCluster(),
                rel.getTraitSet().replace(out),
                convert(
                        project.getInput(),
                        project.getInput().getTraitSet().replace(out)),
                project.getProjects(),
                project.getRowType());
    }
}

