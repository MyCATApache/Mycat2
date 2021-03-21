/**
 * Copyright (C) <2020>  <chen junwen>
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

import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Implementation of {@link RepeatUnion} in
 * {@link MycatConvention Mycat calling convention}.
 */
public class MycatRepeatUnion extends RepeatUnion implements MycatRel {


    /**
     * Creates an EnumerableRepeatUnion.
     */
    public MycatRepeatUnion(RelOptCluster cluster, RelTraitSet traitSet,
                            RelNode seed, RelNode iterative, boolean all, int iterationLimit) {
        super(cluster, traitSet, seed, iterative, all, iterationLimit);
    }

    @Override public MycatRepeatUnion copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;
        return new MycatRepeatUnion(getCluster(), traitSet,
                inputs.get(0), inputs.get(1), all, iterationLimit);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatRepeatUnion").into();
        for (RelNode relNode : getInputs()) {
            MycatRel relNode1 = (MycatRel) relNode;
            relNode1.explain(writer);
        }
        return writer.ret();
    }



    @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

        // return repeatUnion(<seedExp>, <iterativeExp>, iterationLimit, all, <comparer>);

        BlockBuilder builder = new BlockBuilder();
        RelNode seed = getSeedRel();
        RelNode iteration = getIterativeRel();

        Result seedResult = implementor.visitChild(this, 0, (EnumerableRel) seed, pref);
        Result iterationResult = implementor.visitChild(this, 1, (EnumerableRel) iteration, pref);

        Expression seedExp = toEnumerate(builder.append("seed", seedResult.block));
        Expression iterativeExp = toEnumerate(builder.append("iteration", iterationResult.block));

        PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                getRowType(),
                pref.prefer(seedResult.format));

        Expression unionExp = Expressions.call(
                BuiltInMethod.REPEAT_UNION.method,
                seedExp,
                iterativeExp,
                Expressions.constant(iterationLimit, int.class),
                Expressions.constant(all, boolean.class),
                Util.first(physType.comparer(), Expressions.call(BuiltInMethod.IDENTITY_COMPARER.method)));
        builder.add(unionExp);

        return implementor.result(physType, builder.toBlock());
    }


}