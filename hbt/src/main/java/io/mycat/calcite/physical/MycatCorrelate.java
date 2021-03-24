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
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;

public class MycatCorrelate extends Correlate implements MycatRel {
    protected MycatCorrelate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
    }

    public static MycatCorrelate create(RelTraitSet traitSet,
                                 RelNode left,
                                 RelNode right,
                                 CorrelationId correlationId,
                                 ImmutableBitSet requiredColumns,
                                 JoinRelType joinType){
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableCorrelate(mq, left, right,joinType));
        return new MycatCorrelate(cluster,traitSet,left,right,correlationId,requiredColumns,joinType);
    }
    @Override
    public Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        return new MycatCorrelate(
                getCluster(),
                traitSet.replace(MycatConvention.INSTANCE),
                left,
                right,
                correlationId,
                requiredColumns,
                joinType);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
         writer.name("MycatCorrelate");
        for (RelNode input : this.getInputs()) {
            MycatRel mycatRel = (MycatRel) input;
            mycatRel.explain(writer);
        }

        return writer.ret();
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final Result leftResult =
                implementor.visitChild(this, 0, (EnumerableRel) left, pref);
        Expression leftExpression =
                toEnumerate(builder.append(
                        "left", leftResult.block));

        final BlockBuilder corrBlock = new BlockBuilder();
        Type corrVarType = leftResult.physType.getJavaRowType();
        ParameterExpression corrRef; // correlate to be used in inner loop
        ParameterExpression corrArg; // argument to correlate lambda (must be boxed)
        if (!Primitive.is(corrVarType)) {
            corrArg =
                    Expressions.parameter(Modifier.FINAL,
                            corrVarType, getCorrelVariable());
            corrRef = corrArg;
        } else {
            corrArg =
                    Expressions.parameter(Modifier.FINAL,
                            Primitive.box(corrVarType), "$box" + getCorrelVariable());
            corrRef = (ParameterExpression) corrBlock.append(getCorrelVariable(),
                    Expressions.unbox(corrArg));
        }

        implementor.registerCorrelVariable(getCorrelVariable(), corrRef,
                corrBlock, leftResult.physType);

        final Result rightResult =
                implementor.visitChild(this, 1, (EnumerableRel) right, pref);

        implementor.clearCorrelVariable(getCorrelVariable());

        corrBlock.add(rightResult.block);

        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer(JavaRowFormat.ARRAY));

        Expression selector =
                EnumUtils.joinSelector(
                        joinType, physType,
                        ImmutableList.of(leftResult.physType, rightResult.physType));

        builder.append(
                Expressions.call(leftExpression, BuiltInMethod.CORRELATE_JOIN.method,
                        Expressions.constant(EnumUtils.toLinq4jJoinType(joinType)),
                        Expressions.lambda(corrBlock.toBlock(), corrArg),
                        selector));

        return implementor.result(physType, builder.toBlock());
    }
    @Override
    public boolean isSupportStream() {
        return false;
    }
}