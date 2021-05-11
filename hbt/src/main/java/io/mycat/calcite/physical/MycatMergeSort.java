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
import com.google.common.collect.Iterators;
import io.mycat.calcite.*;
import io.mycat.calcite.logical.MycatView;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.RxBuiltInMethodImpl;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.util.*;

public class MycatMergeSort extends AbstractRelNode implements MycatRel {

    //    private static final Method ORDER_BY_METHOD = Types.lookupMethod(MycatMergeSort.class,
//            "orderBy", List.class,
//            Function1.class, Comparator.class, int.class, int.class);
//    private static final Method STREAM_ORDER_BY = Types.lookupMethod(MycatMergeSort.class,
//            "streamOrderBy", List.class,
//            Function1.class, Comparator.class, int.class, int.class);
    public RelCollation collation;
    public RexNode offset;
    public RexNode fetch;
    public RelNode child;

    public MycatMergeSort(RelOptCluster cluster,
                          RelTraitSet traitSet,
                          RelNode child,
                          RelCollation collation,
                          RexNode offset,
                          RexNode fetch) {
        super(cluster, Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE).replace(RelCollationTraitDef.INSTANCE, ImmutableList.of(collation)));
        this.child = child;
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;
        this.rowType = child.getRowType();
    }

    public MycatMergeSort(RelInput relInput) {
        this(relInput.getCluster(), relInput.getTraitSet().plus(relInput.getCollation()),
                relInput.getInput(),
                RelCollationTraitDef.INSTANCE.canonize(relInput.getCollation()),
                relInput.getExpression("offset"), relInput.getExpression("fetch"));
    }

    public static MycatMergeSort create(RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        return new MycatMergeSort(
                child.getCluster(),
                traits.replace(MycatConvention.INSTANCE),
                child,
                collation,
                offset,
                fetch
        );
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatMergeSort").into();
        for (RelNode relNode : getInputs()) {
            MycatRel relNode1 = (MycatRel) relNode;
            relNode1.explain(writer);
        }
        return writer.ret();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        RelCollation trait = traitSet.getTrait(RelCollationTraitDef.INSTANCE);
        if (trait.equals(collation)) {
            return MycatMergeSort.create(traitSet, inputs.get(0), collation, offset, fetch);
        } else {
            throw new UnsupportedOperationException();
        }
    }
//
//    @Override
//    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
//        return new MycatMergeSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
//    }
//
//    final static Method GET_ENUMERABLES =
//            Types.lookupMethod(NewMycatDataContext.class,
//                    "getEnumerables", org.apache.calcite.rel.RelNode.class);
//    final static Method GET_OBSERVABLES =
//            Types.lookupMethod(NewMycatDataContext.class,
//                    "getObservables", org.apache.calcite.rel.RelNode.class);


    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Higher cost if rows are wider discourages pushing a project through an
        // exchange.
        double rowCount = mq.getRowCount(this);
        double bytesPerRow = getRowType().getFieldCount() * 4;
        return planner.getCostFactory().makeCost(
                Util.nLogN(rowCount) * bytesPerRow, rowCount, 0);
//        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public boolean isSupportStream() {
        return true;
    }
}