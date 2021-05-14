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
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.util.Objects;


public class MycatTopN extends Sort implements MycatRel {

    protected MycatTopN(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster,Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE).replace(RelCollationTraitDef.INSTANCE, ImmutableList.of(collation)), child, collation, offset, fetch);
    }

    public static MycatTopN create(RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        RelOptCluster cluster = child.getCluster();
        return new MycatTopN(
                cluster,
                traits.replace(MycatConvention.INSTANCE),
                child,
                collation,
                offset,
                fetch
        );
    }
    public MycatTopN(RelInput input) {
        this(input.getCluster(), input.getTraitSet().plus(input.getCollation()),
                input.getInput(),
                RelCollationTraitDef.INSTANCE.canonize(input.getCollation()),
                input.getExpression("offset"), input.getExpression("fetch"));
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatTopN").into();
        MycatRel input = (MycatRel) this.getInput();

        if (fetch != null) {
            writer.item("fetch", fetch);
        }

        if (offset != null) {
            writer.item("offset", offset);
        }

        input.explain(writer);
        return writer.ret();
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new MycatTopN(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableRel child = (EnumerableRel) this.getInput();
        final Result result = implementor.visitChild(this, 0, child, pref);
        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                this.getRowType(),
                result.format);
        final Expression childExp = toEnumerate(builder.append("child", result.block));

        final PhysType inputPhysType = result.physType;
        final Pair<Expression, Expression> pair =
                inputPhysType.generateCollationKey(this.collation.getFieldCollations());

        final Expression fetchVal;
        if (this.fetch == null) {
            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
        } else {
            fetchVal = getExpression(this.fetch);
        }

        final Expression offsetVal = this.offset == null ? Expressions.constant(Integer.valueOf(0))
                : getExpression(this.offset);

        builder.add(
                Expressions.return_(
                        null, Expressions.call(
                                BuiltInMethod.ORDER_BY_WITH_FETCH_AND_OFFSET.method, Expressions.list(
                                        childExp,
                                        builder.append("keySelector", pair.left))
                                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))
                                        .appendIfNotNull(
                                                builder.appendIfNotNull("offset",
                                                        Expressions.constant(offsetVal)))
                                        .appendIfNotNull(
                                                builder.appendIfNotNull("fetch",
                                                        Expressions.constant(fetchVal)))
                        )));
        return implementor.result(physType, builder.toBlock());
    }

    public static Expression getExpression(RexNode rexNode) {
        if (rexNode instanceof RexDynamicParam) {
            final RexDynamicParam param = (RexDynamicParam) rexNode;
            return Expressions.convert_(
                    Expressions.call(DataContext.ROOT,
                            BuiltInMethod.DATA_CONTEXT_GET.method,
                            Expressions.constant("?" + param.getIndex())),
                    Integer.class);
        } else {
            return Expressions.constant(RexLiteral.intValue(rexNode));
        }
    }
    @Override
    public boolean isSupportStream() {
        return false;
    }
}