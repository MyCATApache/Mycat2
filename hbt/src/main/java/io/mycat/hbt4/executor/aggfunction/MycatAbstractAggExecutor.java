package io.mycat.hbt4.executor.aggfunction;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MycatAbstractAggExecutor {
    final List<Grouping> groups = new ArrayList<>();
    final ImmutableBitSet unionGroups;
    final int outputRowLength;
    final ImmutableList<AccumulatorFactory> accumulatorFactories;
    final Aggregate rel;
    final Executor input;

    public MycatAbstractAggExecutor(Executor input, Aggregate rel) {
        this.input = input;
        this.rel = rel;
        ImmutableBitSet union = ImmutableBitSet.of();

        if (rel.getGroupSets() != null) {
            for (ImmutableBitSet group : rel.getGroupSets()) {
                union = union.union(group);
                groups.add(new Grouping(group));
            }
        }

        this.unionGroups = union;
        this.outputRowLength = unionGroups.cardinality()
                + rel.getAggCallList().size();

        ImmutableList.Builder<AccumulatorFactory> builder = ImmutableList.builder();
        for (AggregateCall aggregateCall : rel.getAggCallList()) {
            builder.add(getAccumulator(aggregateCall, false));
        }
        accumulatorFactories = builder.build();
    }

    /**
     * Internal class to track groupings.
     */
    protected class Grouping {
        private final ImmutableBitSet grouping;
        private final Map<Row, AccumulatorList> accumulators = new HashMap<>();

        private Grouping(ImmutableBitSet grouping) {
            this.grouping = grouping;
        }

        public void send(Row row) {
            // TODO: fix the size of this row.
            Row key = Row.create(grouping.cardinality());
            int j = 0;
            for (Integer i : grouping) {
                key.set(j++, row.getObject(i));
            }

            if (!accumulators.containsKey(key)) {
                AccumulatorList list = new AccumulatorList();
                for (AccumulatorFactory factory : accumulatorFactories) {
                    list.add(factory.get());
                }
                accumulators.put(key, list);
            }

            accumulators.get(key).send(row);
        }

        public Stream<Row> end() {
            return accumulators.entrySet().stream().map(e -> {
                final Row key = e.getKey();
                final AccumulatorList list = e.getValue();
                Row rb = Row.create(outputRowLength);
                int index = 0;
                for (Integer groupPos : unionGroups) {
                    if (grouping.get(groupPos)) {
                        rb.set(index, key.getObject(index));
                    }
                    // need to set false when not part of grouping set.
                    index++;
                }
                list.end(rb);
                return rb;
            });
        }
    }

    public static AccumulatorFactory getAccumulator(final AggregateCall call,
                                                    boolean ignoreFilter) {
        if (call.filterArg >= 0 && !ignoreFilter) {
            final AccumulatorFactory factory = getAccumulator(call, true);
            return () -> {
                final Accumulator accumulator = factory.get();
                return new FilterAccumulator(accumulator, call.filterArg);
            };
        }
        if (call.getAggregation() == SqlStdOperatorTable.COUNT) {
            return () -> new CountAccumulator(call);
        } else if (call.getAggregation() == SqlStdOperatorTable.SUM
                || call.getAggregation() == SqlStdOperatorTable.SUM0) {
            final Class<?> clazz;
            switch (call.type.getSqlTypeName()) {
                case DOUBLE:
                case REAL:
                case FLOAT:
                    clazz = DoubleSum.class;
                    break;
                case DECIMAL:
                    clazz = BigDecimalSum.class;
                    break;
                case INTEGER:
                    clazz = IntSum.class;
                    break;
                case BIGINT:
                default:
                    clazz = LongSum.class;
                    break;
            }
            if (call.getAggregation() == SqlStdOperatorTable.SUM) {
                return new UdaAccumulatorFactory(
                        AggregateFunctionImpl.create(clazz), call, true);
            } else {
                return new UdaAccumulatorFactory(
                        AggregateFunctionImpl.create(clazz), call, false);
            }
        } else if (call.getAggregation() == SqlStdOperatorTable.MIN) {
            final Class<?> clazz;
            switch (call.getType().getSqlTypeName()) {
                case INTEGER:
                    clazz = MinInt.class;
                    break;
                case FLOAT:
                    clazz = MinFloat.class;
                    break;
                case DOUBLE:
                case REAL:
                    clazz = MinDouble.class;
                    break;
                case DECIMAL:
                    clazz = MinBigDecimal.class;
                    break;
                case BOOLEAN:
                    clazz = MinBoolean.class;
                    break;
                default:
                    clazz = MinLong.class;
                    break;
            }
            return new UdaAccumulatorFactory(
                    AggregateFunctionImpl.create(clazz), call, true);
        } else if (call.getAggregation() == SqlStdOperatorTable.MAX) {
            final Class<?> clazz;
            switch (call.getType().getSqlTypeName()) {
                case INTEGER:
                    clazz = MaxInt.class;
                    break;
                case FLOAT:
                    clazz = MaxFloat.class;
                    break;
                case DOUBLE:
                case REAL:
                    clazz = MaxDouble.class;
                    break;
                case DECIMAL:
                    clazz = MaxBigDecimal.class;
                    break;
                default:
                    clazz = MaxLong.class;
                    break;
            }
            return new UdaAccumulatorFactory(
                    AggregateFunctionImpl.create(clazz), call, true);
        } else if (call.getAggregation() == SqlStdOperatorTable.AVG) {
            return () -> new AvgAccumulator(call);
        } else if (call.getAggregation() == SqlStdOperatorTable.SINGLE_VALUE) {
            return () -> new SingleValueAccumulator(call);
        } else if (call.getAggregation() == SqlStdOperatorTable.ANY_VALUE) {
            return () -> new AnyValueAccumulator(call);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}