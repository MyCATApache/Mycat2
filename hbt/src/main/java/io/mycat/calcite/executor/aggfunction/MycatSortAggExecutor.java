///**
// * Copyright (C) <2020>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.calcite.executor.aggfunction;
//
//import io.mycat.calcite.BaseExecutorImplementor;
//import io.mycat.calcite.Executor;
//import io.mycat.calcite.ExplainWriter;
//import io.mycat.mpp.Row;
//import org.apache.calcite.linq4j.Enumerable;
//import org.apache.calcite.linq4j.Linq4j;
//import org.apache.calcite.linq4j.function.Function0;
//import org.apache.calcite.linq4j.function.Function1;
//import org.apache.calcite.linq4j.function.Function2;
//import org.apache.calcite.rel.RelCollation;
//import org.apache.calcite.rel.core.Aggregate;
//import org.apache.calcite.util.ImmutableBitSet;
//
//import java.util.Comparator;
//import java.util.Iterator;
//
//import static org.apache.calcite.linq4j.EnumerableDefaults.sortedGroupBy;
//
//public class MycatSortAggExecutor extends MycatAbstractAggExecutor implements Executor {
//    private Iterator<Row> iter;
//
//    protected MycatSortAggExecutor(Executor input, Aggregate rel) {
//       super(input,rel);
//    }
//
//    public static MycatSortAggExecutor create(Executor input, Aggregate rel) {
//        return new MycatSortAggExecutor(
//                input,
//                rel
//        );
//    }
//
//
//    @Override
//    public void open() {
//        if (iter == null) {
//            input.open();
//            Enumerable<Row> outer = Linq4j.asEnumerable(input);
//            RelCollation collation = rel.getTraitSet().getCollation();
//            Comparator<Row> comparator = BaseExecutorImplementor.comparator(collation.getFieldCollations());
//
//            //不支持groupSets
//            ImmutableBitSet groupSet = rel.getGroupSet();
//            int[] ints = groupSet.toArray();
//            Function1<Row, Row> keySelector = a0 -> {
//                Row row1 = Row.create(ints.length);
//                int index = 0;
//                for (int anInt : ints) {
//                    row1.values[index] = a0.getObject(anInt);
//                    index++;
//                }
//                return row1;
//            };
//            Function0<AccumulatorList> accumulatorInitializer = new Function0<AccumulatorList>() {
//                @Override
//                public AccumulatorList apply() {
//                    AccumulatorList list = new AccumulatorList();
//                    for (AccumulatorFactory factory : accumulatorFactories) {
//                        list.add(factory.get());
//                    }
//                    return list;
//                }
//            };
//            Function2<AccumulatorList, Row, AccumulatorList> accumulatorAdder = new Function2<AccumulatorList, Row, AccumulatorList>() {
//                @Override
//                public AccumulatorList apply(AccumulatorList v0, Row v1) {
//                    v0.send(v1);
//                    return v0;
//                }
//            };
//            final Function2<Row, AccumulatorList, Row> resultSelector = new Function2<Row, AccumulatorList, Row>() {
//                @Override
//                public Row apply(Row key, AccumulatorList list) {
//                    Row rb = Row.create(outputRowLength);
//                    int index = 0;
//                    for (Integer groupPos : unionGroups) {
//                        if (groupSet.get(groupPos)) {
//                            rb.set(index, key.getObject(index));
//                        }
//                        // need to set false when not part of grouping set.
//                        index++;
//                    }
//                    list.end(rb);
//                    return rb;
//                }
//            };
//            iter = sortedGroupBy(outer,
//                    keySelector,
//                    accumulatorInitializer,
//                    accumulatorAdder,
//                    resultSelector,
//                    comparator).iterator();
//        }
//    }
//
//    @Override
//    public Row next() {
//        if (iter.hasNext()) {
//            return iter.next();
//        }
//        return null;
//    }
//
//    @Override
//    public void close() {
//        input.close();
//    }
//
//    @Override
//    public boolean isRewindSupported() {
//        return input.isRewindSupported();
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        ExplainWriter explainWriter = writer.name(this.getClass().getName())
//                .into();
//        input.explain(writer);
//        return explainWriter.ret();
//    }
//
//}