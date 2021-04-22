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


import io.mycat.calcite.*;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.RxBuiltInMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Union operator implemented in Mycat convention.
 */
public class MycatUnion extends Union implements MycatRel {
    protected MycatUnion(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all) {
        super(cluster, Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE), inputs, all);
    }

    public static MycatUnion create(
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all) {
        return new MycatUnion(inputs.get(0).getCluster(), traitSet.replace(MycatConvention.INSTANCE), inputs, all);
    }

    public MycatUnion copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new MycatUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatUnion").into();
        for (RelNode input : getInputs()) {
            MycatRel rel = (MycatRel) input;
            rel.explain(writer);
        }
        return writer.ret();
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        Expression unionExp = null;
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            EnumerableRel input = (EnumerableRel) ord.e;
            final Result result = implementor.visitChild(this, ord.i, input, pref);
            Expression childExp =
                    toEnumerate(builder.append(
                            "child" + ord.i,
                            result.block));

            if (unionExp == null) {
                unionExp = childExp;
            } else {
                unionExp = all
                        ? Expressions.call(unionExp, BuiltInMethod.CONCAT.method, childExp)
                        : Expressions.call(unionExp,
                        BuiltInMethod.UNION.method,
                        Expressions.list(childExp)
                                .appendIfNotNull(result.physType.comparer()));
            }
        }

        builder.add(unionExp);
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer(JavaRowFormat.ARRAY));
        return implementor.result(physType, builder.toBlock());
    }

    @Override
    public boolean isSupportStream() {
        return all;
    }

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        Expression unionExp = null;

        ArrayList<Result> results = new ArrayList<>();
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            EnumerableRel input = (EnumerableRel) ord.e;
            results.add(implementor.visitChild(this, ord.i, input, pref));
        }
        boolean toEnumerate = false;
        for (Result result : results) {
            Type type = result.block.getType();
            toEnumerate |= (!(type instanceof Observable));
        }
        if (toEnumerate){
            for (Ord<Result> ord : Ord.zip(results)) {
                Result result = ord.e;
                Expression childExp =
                        toEnumerate(builder.append(
                                "child" + ord.i,
                                result.block));
                if (unionExp == null) {
                    unionExp = childExp;
                } else {
                    unionExp = all
                            ? Expressions.call(unionExp, BuiltInMethod.CONCAT.method, childExp)
                            : Expressions.call(unionExp,
                            BuiltInMethod.UNION.method,
                            Expressions.list(childExp)
                                    .appendIfNotNull(result.physType.comparer()));
                }
            }
            builder.add(unionExp);
            final PhysType physType =
                    PhysTypeImpl.of(
                            implementor.getTypeFactory(),
                            getRowType(),
                            pref.prefer(JavaRowFormat.ARRAY));
            return implementor.result(physType, builder.toBlock());
        }else {
            for (Ord<Result> ord : Ord.zip(results)) {
                Result result = ord.e;
                Expression childExp =
                        (builder.append(
                                "child" + ord.i,
                                result.block));
                if (unionExp == null) {
                    unionExp = childExp;
                } else  {
                    unionExp = Expressions.call(unionExp, RxBuiltInMethod.OBSERVABLE_UNION_ALL.getMethodName(), childExp);
                }
            }
            builder.add(unionExp);
            final PhysType physType =
                    PhysTypeImpl.of(
                            implementor.getTypeFactory(),
                            getRowType(),
                            pref.prefer(JavaRowFormat.ARRAY));
            return implementor.result(physType, builder.toBlock());
        }
    }
}