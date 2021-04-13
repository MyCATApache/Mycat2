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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.RxBuiltInMethodImpl;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MycatMergeSort extends Sort implements MycatRel {

//    private static final Method ORDER_BY_METHOD = Types.lookupMethod(MycatMergeSort.class,
//            "orderBy", List.class,
//            Function1.class, Comparator.class, int.class, int.class);
//    private static final Method STREAM_ORDER_BY = Types.lookupMethod(MycatMergeSort.class,
//            "streamOrderBy", List.class,
//            Function1.class, Comparator.class, int.class, int.class);

    public MycatMergeSort(RelOptCluster cluster,
                             RelTraitSet traits,
                             RelNode child,
                             RelCollation collation,
                             RexNode offset,
                             RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
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
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new MycatMergeSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }
//
//    final static Method GET_ENUMERABLES =
//            Types.lookupMethod(NewMycatDataContext.class,
//                    "getEnumerables", org.apache.calcite.rel.RelNode.class);
//    final static Method GET_OBSERVABLES =
//            Types.lookupMethod(NewMycatDataContext.class,
//                    "getObservables", org.apache.calcite.rel.RelNode.class);

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatView input = (MycatView) this.getInput();
        return input.implementMergeSort(implementor, pref, this);
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

    public static <TSource, TKey> Observable<TSource> streamOrderBy(
            List<Observable<TSource>> sources,
            Function1<TSource, TKey> keySelector,
            Comparator<TKey> comparator,
            int offset, int fetch) {

        return RxBuiltInMethodImpl.mergeSort(sources, (o1, o2) -> {
            TKey left = keySelector.apply(o1);
            TKey right = keySelector.apply(o2);
            return comparator.compare(left, right);
        }, offset, fetch);
    }

    public static <TSource, TKey> Enumerable<TSource> orderBy(
            List<Enumerable<TSource>> sources,
            Function1<TSource, TKey> keySelector,
            Comparator<TKey> comparator,
            int offset, int fetch) {
        Enumerable<TSource> tSources = Linq4j.asEnumerable(new Iterable<TSource>() {
            @NotNull
            @Override
            public Iterator<TSource> iterator() {
                List<Iterator<TSource>> list = new ArrayList<>();
                for (Enumerable<TSource> source : sources) {
                    list.add(source.iterator());
                }

                return Iterators.<TSource>mergeSorted(list, (o1, o2) -> {
                    TKey left = keySelector.apply(o1);
                    TKey right = keySelector.apply(o2);
                    return comparator.compare(left, right);
                });
            }
        });
        tSources = EnumerableDefaults.skip(tSources, offset);
        tSources = EnumerableDefaults.take(tSources, fetch);
        return tSources;
    }

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatView input = (MycatView) this.getInput();
        return input.implementMergeSortStream(implementor, pref, this);
    }

    @Override
    public boolean isSupportStream() {
        return true;
    }
}