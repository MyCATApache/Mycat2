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

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import io.mycat.DataNode;
import io.mycat.calcite.*;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.IndexCondition;
import io.mycat.calcite.rewriter.PredicateAnalyzer;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.table.ShardingTable;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

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
        super(input.getCluster(), relTrait);
        this.distribution = Objects.requireNonNull(dataNode);
        this.condition = conditions;
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
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
        public RelNode visit(LogicalSort sort) {
            containsOrder = true;
            return sort;
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
        DataNode dataNode;
        if (distribution.type() == Distribution.Type.BroadCast) {
            GlobalTable globalTable = distribution.getGlobalTables().get(0);
            List<DataNode> globalDataNode = globalTable.getGlobalDataNode();
            dataNode = globalDataNode.get(0);
        } else if (distribution.type() == Distribution.Type.PHY) {
            dataNode = distribution.getNormalTables().get(0).getDataNode();
        } else {
            ShardingTable shardingTable = distribution.getShardingTables().get(0);
            dataNode = shardingTable.dataNodes().get(0);
        }
        String targetName = dataNode.getTargetName();
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
        Stream<Map<String, DataNode>> dataNodes = mycatViewDataNodeMapping.apply(params);
        if (mycatViewDataNodeMapping.getType() == Distribution.Type.BroadCast) {
            GlobalTable globalTable = mycatViewDataNodeMapping.distribution().getGlobalTables().get(0);
            List<DataNode> globalDataNode = globalTable.getGlobalDataNode();
            int i = ThreadLocalRandom.current().nextInt(0, globalDataNode.size());
            DataNode dataNode = globalDataNode.get(i);
            String targetName = dataNode.getTargetName();
            Map<String, DataNode> nodeMap = dataNodes.findFirst().get();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            SqlNode sqlSelectStatement = MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate,params, nodeMap);
            return new MycatViewSqlString(ImmutableMultimap.of(targetName,sqlSelectStatement.toSqlString(dialect) ));
        }
        if (mergeUnionSize == 0 || mycatViewDataNodeMapping.containsOrder()) {
            ImmutableMultimap.Builder<String, SqlString> builder = ImmutableMultimap.builder();
            dataNodes.forEach(m -> {
                String targetName = m.values().iterator().next().getTargetName();
                SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
                SqlString sqlString = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate,params, m),(dialect));
                builder.put(targetName, sqlString);
            });
            return new MycatViewSqlString(builder.build());
        }
        Map<String, List<Map<String, DataNode>>> collect = dataNodes.collect(Collectors.groupingBy(m -> m.values().iterator().next().getTargetName()));
        ImmutableMultimap.Builder<String, SqlString> resMapBuilder = ImmutableMultimap.builder();
        for (Map.Entry<String, List<Map<String, DataNode>>> entry : collect.entrySet()) {
            String targetName = entry.getKey();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            Iterator<List<Map<String, DataNode>>> iterator = Iterables.partition(entry.getValue(), mergeUnionSize + 1).iterator();
            while (iterator.hasNext()) {
                List<Map<String, DataNode>> eachList = iterator.next();
                ImmutableList.Builder<SqlString> builderList = ImmutableList.builder();
                SqlString string = null;
                List<Integer> list = new ArrayList<>();
                for (Map<String, DataNode> each : eachList) {
                    string = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, params, each),dialect);
                    if( string.getDynamicParameters()!=null){
                        list.addAll( string.getDynamicParameters());
                    }
                    builderList.add(string);
                }
                ImmutableList<SqlString> relNodes = builderList.build();
                resMapBuilder.put(targetName,
                        new SqlString(dialect,
                                relNodes.stream().map(i -> i.getSql()).collect(Collectors.joining(" union all ")),
                                ImmutableList.copyOf(list) ));
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
        if (condition!=null) {
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
        return relNode.computeSelfCost(planner, mq);
    }


    private RelNode applyDataNode(Map<String, DataNode> map, RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable mycatLogicTable = scan.getTable().unwrap(MycatLogicTable.class);
                if (mycatLogicTable != null) {
                    String uniqueName = mycatLogicTable.getTable().getUniqueName();
                    DataNode dataNode = map.get(uniqueName);
                    MycatPhysicalTable physicalTable = new MycatPhysicalTable(mycatLogicTable, dataNode);
                    RelOptTableImpl relOptTable1 = RelOptTableImpl.create(scan.getTable().getRelOptSchema(),
                            scan.getRowType(),
                            physicalTable,
                            ImmutableList.of(dataNode.getTargetName(), dataNode.getSchema(), dataNode.getTable())
                    );
                    return LogicalTableScan.create(scan.getCluster(), relOptTable1, ImmutableList.of());
                }
                return super.visit(scan);
            }
        });
    }


    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(getDigest());
        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getEnumerable", String.class);
        builder.add(Expressions.call(root, getEnumerable, mycatViewStash));
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

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
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

    public Result implementMergeSort(MycatEnumerableRelImplementor implementor, Prefer pref, MycatMergeSort mycatMergeSort) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(mycatMergeSort.getDigest());

        final PhysType inputPhysType = physType;
        final Pair<Expression, Expression> pair =
                inputPhysType.generateCollationKey(mycatMergeSort.collation.getFieldCollations());

        final Expression fetchVal;
        if (mycatMergeSort.fetch == null) {
            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
        } else {
            fetchVal = MycatMergeSort.getExpression(mycatMergeSort.fetch);
        }
//        builder.append("keySelector", pair.left))
//                                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))

        final Expression offsetVal = mycatMergeSort.offset == null ? Expressions.constant(Integer.valueOf(0))
                : MycatMergeSort.getExpression(mycatMergeSort.offset);

        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getEnumerable", String.class, Function1.class, Comparator.class, int.class, int.class);
        builder.add(Expressions.call(root, getEnumerable, mycatViewStash, pair.left, pair.right, offsetVal, fetchVal));
        return implementor.result(physType, builder.toBlock());

    }

    public Result implementMergeSortStream(MycatEnumerableRelImplementor implementor, Prefer pref, MycatMergeSort mycatMergeSort) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(this.getDigest());

        final PhysType inputPhysType = physType;
        final Pair<Expression, Expression> pair =
                inputPhysType.generateCollationKey(mycatMergeSort.collation.getFieldCollations());

        final Expression fetchVal;
        if (mycatMergeSort.fetch == null) {
            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
        } else {
            fetchVal = MycatMergeSort.getExpression(mycatMergeSort.fetch);
        }
//        builder.append("keySelector", pair.left))
//                                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))

        final Expression offsetVal = mycatMergeSort.offset == null ? Expressions.constant(Integer.valueOf(0))
                : MycatMergeSort.getExpression(mycatMergeSort.offset);
        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class, Function1.class, Comparator.class, int.class, int.class);
        final Expression expression2 = Expressions.call(root, getEnumerable, mycatViewStash, pair.left, pair.right, offsetVal, fetchVal);
        builder.add(toRows(physType, expression2, getRowType().getFieldCount()));


        return implementor.result(physType, builder.toBlock());
    }
}