/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.physical;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.*;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.adapter.enumerable.EnumUtils.*;

/**
 * Implementation of {@link Project} in
 * {@link MycatConvention Mycat calling convention}.
 */
public class MycatProject
        extends Project
        implements MycatRel, Serializable {
    public MycatProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE), ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof MycatConvention;
    }

    public MycatProject(RelInput relInput) {
        this(relInput.getCluster(),
                relInput.getTraitSet(),
                relInput.getInput(),
                Objects.requireNonNull(relInput.getExpressionList("exprs")),
                Objects.requireNonNull(relInput.getRowType("exprs", "fields")));
    }

    /**
     * Creates an MycatProject, specifying row type rather than field
     * names.
     */
    public static MycatProject create(final RelNode input,
                                      final List<? extends RexNode> projects, RelDataType rowType) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSet().replace(MycatConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.project(mq, input, projects));
        return new MycatProject(cluster, traitSet, input, projects, rowType);
    }


    @Override
    public MycatProject copy(RelTraitSet traitSet, RelNode input,
                             List<RexNode> projects, RelDataType rowType) {
        return new MycatProject(getCluster(), traitSet, input, projects, rowType);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatProject").item("projects", this.exps).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatCalc mycatCalc = toMycatCacl();
        return mycatCalc.implement(implementor, pref);
    }

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        return toMycatCacl().implementStream(implementor, pref);
    }

    @NotNull
    private MycatCalc toMycatCacl() {
        final Project project = this;
        final RelNode input = project.getInput();
        final RexProgram program =
                RexProgram.create(
                        input.getRowType(),
                        project.getProjects(),
                        null,
                        project.getRowType(),
                        project.getCluster().getRexBuilder());
        MycatCalc mycatCalc = MycatCalc.create(getTraitSet(), input, program);
        return mycatCalc;
    }

    @Override
    public boolean isSupportStream() {
        return this.getCorrelVariable() == null;
    }
}