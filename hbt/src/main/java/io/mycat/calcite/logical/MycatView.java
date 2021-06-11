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
package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.mycat.Partition;
import io.mycat.calcite.*;
import io.mycat.calcite.localrel.ToLocalConverter;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.IndexCondition;
import io.mycat.calcite.rewriter.PredicateAnalyzer;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.table.ShardingTable;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.RxBuiltInMethodImpl;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MycatView extends AbstractRelNode implements MycatRel {
    final RelNode relNode;
    final Distribution distribution;
    final RexNode condition;


    public MycatView(RelTraitSet relTrait, RelNode input, Distribution dataNode) {
        this(relTrait, input, dataNode, null);
    }

    public MycatView(RelInput relInput) {
        this(relInput.getTraitSet(), relInput.getInput(), Distribution.fromJson((List) relInput.get("distribution")), relInput.getExpression("condition"));
    }

    public MycatView(RelTraitSet relTrait, RelNode input, Distribution dataNode, RexNode conditions) {
        super(input.getCluster(), relTrait=relTrait.replace(MycatConvention.INSTANCE));
        this.distribution = Objects.requireNonNull(dataNode);
        this.condition = conditions;
        this.rowType = input.getRowType();
        if (input instanceof MycatRel) {
            this.relNode = input.accept(new ToLogicalConverter(MycatCalciteSupport.relBuilderFactory.create(input.getCluster(), null)));
        } else {
            ToLocalConverter toLocalConverter = new ToLocalConverter();
            this.relNode = input.accept(toLocalConverter);
        }
    }


    public static MycatView ofCondition(RelNode input,
                                        Distribution dataNodeInfo,
                                        RexNode conditions) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo, conditions);
    }

    public MycatView changeTo(RelNode input, Distribution dataNodeInfo) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo, this.condition);
    }

    public MycatView changeTo(RelNode input) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, distribution, this.condition);
    }

    public static MycatView ofBottom(RelNode input, Distribution dataNodeInfo) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo);
    }


    static class FindOrder extends RelShuttleImpl {
        boolean containsOrder = false;

        @Override
        protected RelNode visitChildren(RelNode rel) {
            if (rel instanceof Sort){
                containsOrder = true;
            }
            return super.visitChildren(rel);
        }
    }


    public MycatViewDataNodeMapping getMycatViewDataNodeMapping() {
        FindOrder findOrder = new FindOrder();
        relNode.accept(findOrder);
        boolean containsOrder = findOrder.containsOrder;
        Distribution.Type type = distribution.type();
        switch (type) {
            case BroadCast:
            case PHY:
                return new MycatViewDataNodeMappingImpl(containsOrder, distribution.toNameList(), IndexCondition.EMPTY);
            case Sharding:
                ShardingTable shardingTable = distribution.getShardingTables().get(0);
                PredicateAnalyzer predicateAnalyzer = new PredicateAnalyzer(shardingTable.keyMetas(), shardingTable.getColumns().stream().map(i -> i.getColumnName()).collect(Collectors.toList()));
                IndexCondition indexCondition = predicateAnalyzer.translateMatch(condition);
                return new MycatViewDataNodeMappingImpl(containsOrder, distribution.toNameList(), indexCondition);
            default:
                throw new IllegalStateException("Unexpected value: " + distribution.type());
        }
    }

    public SqlNode getSQLTemplate(boolean update) {
        Partition partition;
        if (distribution.type() == Distribution.Type.BroadCast) {
            GlobalTable globalTable = distribution.getGlobalTables().get(0);
            List<Partition> globalPartition = globalTable.getGlobalDataNode();
            partition = globalPartition.get(0);
        } else if (distribution.type() == Distribution.Type.PHY) {
            partition = distribution.getNormalTables().get(0).getDataNode();
        } else {
            ShardingTable shardingTable = distribution.getShardingTables().get(0);
            partition = shardingTable.dataNodes().get(0);
        }
        String targetName = partition.getTargetName();
        SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
        return MycatCalciteSupport.INSTANCE.convertToSqlTemplate(relNode, dialect, update);
    }

    /**
     * ImmutableMultimap<String, SQLSelectStatement>
     *
     * @param mycatViewDataNodeMapping
     * @param sqlTemplateArg
     * @param params
     * @param mergeUnionSize
     * @return
     */
    public static MycatViewSqlString apply(MycatViewDataNodeMapping mycatViewDataNodeMapping,
                                           SqlNode sqlTemplateArg,
                                           List<Object> params,
                                           int mergeUnionSize) {
        SqlNode sqlTemplate = sqlTemplateArg;
        Stream<Map<String, Partition>> dataNodes = mycatViewDataNodeMapping.apply(params);
        if (mycatViewDataNodeMapping.getType() == Distribution.Type.BroadCast) {
            GlobalTable globalTable = mycatViewDataNodeMapping.distribution().getGlobalTables().get(0);
            List<Partition> globalPartition = globalTable.getGlobalDataNode();
            int i = ThreadLocalRandom.current().nextInt(0, globalPartition.size());
            Partition partition = globalPartition.get(i);
            String targetName = partition.getTargetName();
            Map<String, Partition> nodeMap = dataNodes.findFirst().get();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            SqlNode sqlSelectStatement = MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, params, nodeMap);
            return new MycatViewSqlString(ImmutableMultimap.of(targetName, sqlSelectStatement.toSqlString(dialect)));
        }
        if (mergeUnionSize == 0 || mycatViewDataNodeMapping.containsOrder()) {
            ImmutableMultimap.Builder<String, SqlString> builder = ImmutableMultimap.builder();
            dataNodes.forEach(m -> {
                String targetName = m.values().iterator().next().getTargetName();
                SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
                SqlString sqlString = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, params, m), (dialect));
                builder.put(targetName, sqlString);
            });
            return new MycatViewSqlString(builder.build());
        }
        Map<String, List<Map<String, Partition>>> collect = dataNodes.collect(Collectors.groupingBy(m -> m.values().iterator().next().getTargetName()));
        ImmutableMultimap.Builder<String, SqlString> resMapBuilder = ImmutableMultimap.builder();
        for (Map.Entry<String, List<Map<String, Partition>>> entry : collect.entrySet()) {
            String targetName = entry.getKey();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            Iterator<List<Map<String, Partition>>> iterator = Iterables.partition(entry.getValue(), mergeUnionSize + 1).iterator();
            while (iterator.hasNext()) {
                List<Map<String, Partition>> eachList = iterator.next();
                ImmutableList.Builder<SqlString> builderList = ImmutableList.builder();
                SqlString string = null;
                List<Integer> list = new ArrayList<>();
                for (Map<String, Partition> each : eachList) {
                    string = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, params, each), dialect);
                    if (string.getDynamicParameters() != null) {
                        list.addAll(string.getDynamicParameters());
                    }
                    builderList.add(string);
                }
                ImmutableList<SqlString> relNodes = builderList.build();
                resMapBuilder.put(targetName,
                        new SqlString(dialect,
                                relNodes.stream().map(i -> i.getSql()).collect(Collectors.joining(" union all ")),
                                ImmutableList.copyOf(list)));
            }
        }
        return new MycatViewSqlString(resMapBuilder.build());
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatView(traitSet, relNode, distribution);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        writer.item("relNode", relNode);
        writer.item("distribution", distribution.toNameList());
        if (condition != null) {
            writer.item("conditions", condition);
        }
        return writer;
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("View").into().item("sql", getSql(MycatSqlDialect.DEFAULT)).ret();
    }

    public String getSql() {
        return getSql(MycatSqlDialect.DEFAULT).toString();
    }

    public SqlNode getSql(SqlDialect dialect) {
        return MycatCalciteSupport.INSTANCE.convertToSqlTemplate(relNode, dialect, false);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCost(relNode,mq);
    }
    private RelNode applyDataNode(Map<String, Partition> map, RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable mycatLogicTable = scan.getTable().unwrap(MycatLogicTable.class);
                if (mycatLogicTable != null) {
                    String uniqueName = mycatLogicTable.getTable().getUniqueName();
                    Partition partition = map.get(uniqueName);
                    MycatPhysicalTable physicalTable = new MycatPhysicalTable(mycatLogicTable, partition);
                    RelOptTableImpl relOptTable1 = RelOptTableImpl.create(scan.getTable().getRelOptSchema(),
                            scan.getRowType(),
                            physicalTable,
                            ImmutableList.of(partition.getTargetName(), partition.getSchema(), partition.getTable())
                    );
                    return LogicalTableScan.create(scan.getCluster(), relOptTable1, ImmutableList.of());
                }
                return super.visit(scan);
            }
        });
    }


    public Result implementView(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(getDigest());
        Method getObservable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class);
        builder.add(Expressions.call(root, getObservable, mycatViewStash));
        return implementor.result(physType, builder.toBlock());
    }


    public static Expression toEnumerable(Expression expression) {
        final Type type = expression.getType();
        if (Types.isArray(type)) {
            if (Types.toClass(type).getComponentType().isPrimitive()) {
                expression =
                        Expressions.call(BuiltInMethod.AS_LIST.method, expression);
            }
            return Expressions.call(BuiltInMethod.AS_ENUMERABLE.method, expression);
        } else if (Types.isAssignableFrom(Iterable.class, type)
                && !Types.isAssignableFrom(Enumerable.class, type)) {
            return Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method,
                    expression);
        } else if (Types.isAssignableFrom(Queryable.class, type)) {
            // Queryable extends Enumerable, but it's too "clever", so we call
            // Queryable.asEnumerable so that operations such as take(int) will be
            // evaluated directly.
            return Expressions.call(expression,
                    BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);
        }
        return expression;
    }

    public static Expression toRows(PhysType physType, Expression expression, final int fieldCount) {
        JavaRowFormat oldFormat = JavaRowFormat.ARRAY;
        if (physType.getFormat() == oldFormat) {
            return expression;
        }
        final ParameterExpression row_ =
                Expressions.parameter(Object[].class, "row");
        List<Expression> expressionList = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            expressionList.add(fieldExpression(row_, i, physType, oldFormat));
        }
        return Expressions.call(expression,
                BuiltInMethod.SELECT.method,
                Expressions.lambda(Function1.class, physType.record(expressionList),
                        row_));
    }

    public static Expression fieldExpression(ParameterExpression row_, int i,
                                             PhysType physType, JavaRowFormat format) {
        return format.field(row_, i, null, physType.getJavaFieldType(i));
    }

    @Override
    public boolean isSupportStream() {
        return true;
    }


    public Result implementViewStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(getDigest());
        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class);
        final Expression expression2 = Expressions.call(root, getEnumerable, mycatViewStash);
        builder.add(toRows(physType, expression2, getRowType().getFieldCount()));
        return implementor.result(physType, builder.toBlock());
    }

    public Optional<RexNode> getCondition() {
        return Optional.ofNullable(condition);
    }

    public Result implementMergeSort(MycatEnumerableRelImplementor implementor, Prefer pref, RelNode relNode) {
        MycatMergeSort mycatMergeSort = null;
        MycatView view = (MycatView) relNode;
        if (view.getDistribution().type() == Distribution.Type.Sharding) {
            if (view.getRelNode() instanceof Sort) {
                Sort viewRelNode = (Sort) view.getRelNode();
                RexNode rexNode = (RexNode) viewRelNode.fetch;
                if (rexNode != null && rexNode.getKind() == SqlKind.PLUS) {
                    RexCall plus = (RexCall) rexNode;
                    mycatMergeSort = MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), plus.getOperands().get(0), plus.getOperands().get(1));
                } else {
                    mycatMergeSort = MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), viewRelNode.offset, viewRelNode.fetch);
                }
            }
        } else {
            throw new IllegalArgumentException();
        }
//            MycatView view = (MycatView) relNode;
//            if (view.getDistribution().type() == Distribution.Type.Sharding) {
//                if (view.getRelNode() instanceof LogicalSort) {
//                    LogicalSort viewRelNode = (LogicalSort) view.getRelNode();
//                    RexNode rexNode = (RexNode) viewRelNode.fetch;
//                    if (rexNode != null && rexNode.getKind() == SqlKind.PLUS) {
//                        RexCall plus = (RexCall) rexNode;
//                        return MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), plus.getOperands().get(0), plus.getOperands().get(1));
//                    } else {
//                        return MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), viewRelNode.offset, viewRelNode.fetch);
//                    }
//                }
//            }
//        }

        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(relNode.getDigest());

        final PhysType inputPhysType = physType;
        final Pair<Expression, Expression> pair =
                inputPhysType.generateCollationKey(mycatMergeSort.collation.getFieldCollations());

        final Expression fetchVal;
        if (mycatMergeSort.fetch == null) {
            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
        } else {
            fetchVal = getExpression(mycatMergeSort.fetch);
        }
//        builder.append("keySelector", pair.left))
//                                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))

        final Expression offsetVal = mycatMergeSort.offset == null ? Expressions.constant(Integer.valueOf(0))
                : getExpression(mycatMergeSort.offset);

        Method getObservable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class, Function1.class, Comparator.class, int.class, int.class);
        builder.add(Expressions.call(root, getObservable, mycatViewStash, pair.left, pair.right, offsetVal, fetchVal));
        return implementor.result(physType, builder.toBlock());

    }
//
//    public Result implementMergeSortStream(MycatEnumerableRelImplementor implementor, Prefer pref, MycatMergeSort mycatMergeSort) {
//        final BlockBuilder builder = new BlockBuilder();
//        final PhysType physType =
//                PhysTypeImpl.of(
//                        implementor.getTypeFactory(),
//                        getRowType(),
//                        JavaRowFormat.ARRAY);
//        ParameterExpression root = implementor.getRootExpression();
//        Expression mycatViewStash = Expressions.constant(mycatMergeSort.getDigest());
//
//        final PhysType inputPhysType = physType;
//        final Pair<Expression, Expression> pair =
//                inputPhysType.generateCollationKey(mycatMergeSort.collation.getFieldCollations());
//
//        final Expression fetchVal;
//        if (mycatMergeSort.fetch == null) {
//            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
//        } else {
//            fetchVal = getExpression(mycatMergeSort.fetch);
//        }
////        builder.append("keySelector", pair.left))
////                                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))
//
//        final Expression offsetVal = mycatMergeSort.offset == null ? Expressions.constant(Integer.valueOf(0))
//                : getExpression(mycatMergeSort.offset);
//        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class, Function1.class, Comparator.class, int.class, int.class);
//        final Expression expression2 = Expressions.call(root, getEnumerable, mycatViewStash, pair.left, pair.right, offsetVal, fetchVal);
//        builder.add(toRows(physType, expression2, getRowType().getFieldCount()));
//
//
//        return implementor.result(physType, builder.toBlock());
//    }

    public boolean isMergeSort() {
        MycatView view = this;
        if (view.getDistribution().type() == Distribution.Type.Sharding) {
            return (view.getRelNode() instanceof Sort);
        } else {
            return false;
        }
    }

    public boolean isMergeAgg() {
        MycatView view = this;
        if (view.getDistribution().type() == Distribution.Type.Sharding) {
            return (view.getRelNode() instanceof Aggregate);
        } else {
            return false;
        }
    }


    public Result implementMergeView(MycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatView input = (MycatView) this;
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

    public static <TSource, TKey> io.reactivex.rxjava3.core.Observable<TSource> streamOrderBy(
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


    public Result implementMergeViewStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatView input = this;
        return implementMergeSort(implementor, pref, input);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        if (isMergeSort()) {
            return implementMergeView((MycatEnumerableRelImplementor) implementor, pref);
        }
        return implementView((MycatEnumerableRelImplementor) implementor, pref);
    }

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        if (isMergeSort()) {
            return implementMergeViewStream(implementor, pref);
        }
        return implementViewStream(implementor, pref);
    }
}