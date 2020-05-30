package io.mycat.mpp.plan;

import io.mycat.mpp.AggregationCallExp;
import io.mycat.mpp.AggregationGroup;
import io.mycat.mpp.AggregationKey;
import io.mycat.mpp.DataContext;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;


public class AggregationPlan extends NodePlan {
    private final String[] aggCallNames;
    private final RowType returnType;
    private final List<List<Integer>> args;
    private final int[] groupedFieldsIndexes;//多个键
    private final boolean concurrent;
    private final String[] resultSetColumnNames;

    public AggregationPlan(QueryPlan from,
                           String[] aggCallNames,
                           RowType returnType,
                           List<List<Integer>> args,
                           int[] groupedFieldsIndexes,
                           boolean concurrent) {
        super(from);
        this.aggCallNames = aggCallNames;
        this.returnType = returnType;
        this.args = args;
        this.groupedFieldsIndexes = groupedFieldsIndexes;
        this.concurrent = concurrent;

        this.resultSetColumnNames = Arrays.stream(returnType.getColumns()).map(i -> i.getName()).toArray(String[]::new);
    }

    public static final AggregationPlan create(QueryPlan from,
                                               String[] aggCallNames,
                                               List<List<Integer>> args,
                                               RowType returnType,
                                               int[] groupedFieldsIndexes) {
        return create(from, aggCallNames, returnType, args, groupedFieldsIndexes, false);
    }

    public static final AggregationPlan create(QueryPlan from,
                                               String[] aggCallNames,
                                               RowType returnType,
                                               List<List<Integer>> args,
                                               int[] groupedFieldsIndexes,
                                               boolean concurrent) {
        return new AggregationPlan(from, aggCallNames, returnType, args, groupedFieldsIndexes, concurrent);
    }

    @Override
    public RowType getType() {
        return returnType;
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        if (groupedFieldsIndexes != null && groupedFieldsIndexes.length > 0) {
            return Scanner.of(new Iterator<DataAccessor>() {
                private volatile Iterator<Map.Entry<AggregationKey, AggregationGroup>> iterator;

                private synchronized void compute() {
                    Scanner scan = from.scan(dataContext, flags);
                    final Map<AggregationKey, AggregationGroup> groupsMap = groupBy(scan, groupedFieldsIndexes, aggCallNames, resultSetColumnNames, args, concurrent);
                    Stream<Map.Entry<AggregationKey, AggregationGroup>> stream = groupsMap.entrySet().stream();
                    if (concurrent) {
                        stream = stream.parallel();
                    }
                    this.iterator = stream.iterator();
                }

                @Override
                public boolean hasNext() {
                    if (iterator == null) {
                        compute();
                    }
                    return iterator.hasNext();
                }

                @Override
                public DataAccessor next() {
                    Map.Entry<AggregationKey, AggregationGroup> next = iterator.next();
                    AggregationKey k = next.getKey();
                    AggregationGroup v = next.getValue();
                    Object[] values = new Object[resultSetColumnNames.length];
                    int index = 0;
                    for (Object field : k.getValues()) values[index++] = field;
                    for (AggregationCallExp column : v.getColumns()) values[index++] = column.getValue();
//                    iterator.remove();//help gc
                    return DataAccessor.of(values);
                }
            });
        } else {
            return Scanner.of(new Iterator<DataAccessor>() {
                private volatile AggregationGroup group;

                private void compute() {
                    Scanner scanner = from.scan(dataContext, flags);
                    AggregationGroup group = AggregationGroup.of(aggCallNames, resultSetColumnNames, args);
                    AggregationCallExp[] columns = group.getColumns();
                    while (scanner.hasNext()) {
                        DataAccessor dataAccessor = scanner.next();
                        for (AggregationCallExp cc : columns) {
                            cc.accept(dataAccessor);
                        }
                    }
                    this.group = group;
                }

                @Override
                public synchronized boolean hasNext() {
                    if (group == null) {
                        compute();
                        return true;
                    }
                    return false;
                }

                @Override
                public DataAccessor next() {
                    Object[] values = new Object[resultSetColumnNames.length];
                    int index = 0;
                    for (AggregationCallExp column : group.getColumns()) values[index++] = column.getValue();
                    return DataAccessor.of(values);
                }
            });
        }
    }

    public Map<AggregationKey, AggregationGroup> groupBy(Scanner scan, int[] groupedFieldsIndexes, String[] aggCallNames, String[] columnNames, List<List<Integer>> args, boolean concurrent) {
        EnumSet<Collector.Characteristics> characteristics = getCharacteristics(concurrent);
        Stream<DataAccessor> stream = scan.stream();
        stream = concurrent ? stream.parallel() : stream;
        return stream.collect(new Collector<DataAccessor, Map<AggregationKey, AggregationGroup>, Map<AggregationKey, AggregationGroup>>() {

            @Override
            public Supplier<Map<AggregationKey, AggregationGroup>> supplier() {
                return () -> concurrent ? new ConcurrentHashMap<>() : new HashMap<>();
            }

            @Override
            public BiConsumer<Map<AggregationKey, AggregationGroup>, DataAccessor> accumulator() {
                return new BiConsumer<Map<AggregationKey, AggregationGroup>, DataAccessor>() {
                    private AggregationGroup createGroup(AggregationKey aggregationKey) {
                        return AggregationGroup.of(aggCallNames, columnNames, args);
                    }

                    @Override
                    public void accept(Map<AggregationKey, AggregationGroup> aggregationKeyAggregationGroupMap,
                                       DataAccessor dataAccessor) {
                        AggregationKey key = AggregationKey.of(dataAccessor, groupedFieldsIndexes);
                        AggregationGroup aggregationGroup = aggregationKeyAggregationGroupMap.computeIfAbsent(key, this::createGroup);
                        for (final AggregationCallExp cc : aggregationGroup.getColumns()) {
                            if (concurrent) {
                                synchronized (cc) {
                                    cc.accept(dataAccessor);
                                }
                            } else {
                                cc.accept(dataAccessor);
                            }
                        }
                    }
                };
            }

            @Override
            public BinaryOperator<Map<AggregationKey, AggregationGroup>> combiner() {
                return (m1, m2) -> {
                    for (Map.Entry<AggregationKey, AggregationGroup> e : m2.entrySet())
                        m1.merge(e.getKey(), e.getValue(), AggregationGroup::merge);
                    return m1;

                };
            }

            @Override
            public Function<Map<AggregationKey, AggregationGroup>, Map<AggregationKey, AggregationGroup>> finisher() {
                return aggregationKeyAggregationGroupMap -> aggregationKeyAggregationGroupMap;
            }

            @Override
            public Set<Characteristics> characteristics() {
                return characteristics;
            }
        });
    }

    @NotNull
    public EnumSet<Collector.Characteristics> getCharacteristics(boolean concurrent) {
        return concurrent ?
                EnumSet.of(Collector.Characteristics.CONCURRENT, Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH)
                : EnumSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH);
    }
}