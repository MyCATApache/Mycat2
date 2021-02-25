/**
 * Copyright (C) <2020>  <chen junwen>
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
import io.mycat.DataNode;
import io.mycat.MetaClusterCurrent;
import io.mycat.TableHandler;
import io.mycat.calcite.*;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.PredicateAnalyzer;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.ServerConfig;
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
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.zookeeper.Op;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MycatView extends AbstractRelNode implements MycatRel {
    final RelNode relNode;
    final Distribution distribution;
    final RexNode conditions;
    final boolean containsOrder;

    public MycatView(RelTraitSet relTrait, RelNode input, Distribution dataNode) {
        this(relTrait, input, dataNode, null);
    }

    public MycatView(RelTraitSet relTrait, RelNode input, Distribution dataNode, RexNode conditions) {
        super(input.getCluster(), relTrait);
        this.distribution = Objects.requireNonNull(dataNode);
        this.conditions = conditions;
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
        FindOrder findOrder = new FindOrder();
        this.relNode.accept(findOrder);
        this.containsOrder = findOrder.containsOrder;
    }

    public boolean isContainsOrder() {
        return containsOrder;
    }

    static class FindOrder extends RelShuttleImpl {
        boolean containsOrder = false;

        @Override
        public RelNode visit(LogicalSort sort) {
            containsOrder = true;
            return sort;
        }
    }

    public static MycatView ofCondition(RelNode input,
                                        Distribution dataNodeInfo,
                                        RexNode conditions) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo, conditions);
    }

    public MycatView changeTo(RelNode input, Distribution dataNodeInfo) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo, this.conditions);
    }

    public static MycatView ofBottom(RelNode input, Distribution dataNodeInfo) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo);
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
        writer.item("distribution", distribution);
        writer.item("conditions", conditions);
        return writer;
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("View").into().item("sql", getSql(MycatSqlDialect.DEFAULT)).ret();
    }

    public String getSql(SqlDialect dialect) {
        return MycatCalciteSupport.INSTANCE.convertToSql(relNode, dialect, false).getSql();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }


    public RelNode expandToPhyRelNode() {
        List<Object> params = Collections.emptyList();
        List<RelNode> subViews = assignParamsToRelNode(params);
        return LogicalUnion.create(subViews, true);
    }

    private List<RelNode> assignParamsToRelNode(List<Object> params) {
        return assignParams(params)
                .map(map -> applyDataNode(map, this.relNode))
                .collect(Collectors.toList());
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

    public Stream<Map<String, DataNode>> assignParams(List<Object> params) {
        return distribution.getDataNodes(table -> PredicateAnalyzer.analyze(table, conditions == null ?
                ImmutableList.of() : ImmutableList.of(conditions), params));
    }

    public ImmutableMultimap<String, SqlString> expandToSql(boolean update, List<Object> params) {
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        return expandToSql(update, params, serverConfig.getMergeUnionSize());
    }

    public ImmutableMultimap<String, SqlString> expandToSql(boolean update, List<Object> params, int mergeUnionSize) {
        Stream<Map<String, DataNode>> dataNodes = assignParams(params);
        if (distribution.type() == Distribution.Type.BroadCast) {
            GlobalTable globalTable = distribution.getGlobalTables().get(0);
            List<DataNode> globalDataNode = globalTable.getGlobalDataNode();
            int i = ThreadLocalRandom.current().nextInt(0, globalDataNode.size());
            DataNode dataNode = globalDataNode.get(i);
            String targetName = dataNode.getTargetName();
            Map<String, DataNode> m = dataNodes.findFirst().get();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            SqlString sqlString = MycatCalciteSupport.INSTANCE.convertToSql(relNode, dialect, m, update, params);
            return ImmutableMultimap.of(targetName, sqlString);
        }
        if (mergeUnionSize == 0 || containsOrder) {
            ImmutableMultimap.Builder<String, SqlString> builder = ImmutableMultimap.builder();
            dataNodes.forEach(m -> {
                String targetName = m.values().iterator().next().getTargetName();
                SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
                SqlString sqlString = MycatCalciteSupport.INSTANCE.convertToSql(relNode, dialect, m, update, params);
                builder.put(targetName, sqlString);
            });
            return builder.build();
        }
        Map<String, List<Map<String, DataNode>>> collect = dataNodes.collect(Collectors.groupingBy(m -> m.values().iterator().next().getTargetName()));
        ImmutableMultimap.Builder<String, SqlString> resMapBuilder = ImmutableMultimap.builder();
        for (Map.Entry<String, List<Map<String, DataNode>>> entry : collect.entrySet()) {
            String targetName = entry.getKey();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            Iterator<List<Map<String, DataNode>>> iterator = Iterables.partition(entry.getValue(), mergeUnionSize).iterator();
            while (iterator.hasNext()) {
                List<Map<String, DataNode>> eachList = iterator.next();
                ImmutableList.Builder<RelNode> builderList = ImmutableList.builder();
                for (Map<String, DataNode> each : eachList) {
                    RelNode newNode = relNode.accept(new RelShuttleImpl() {
                        @Override
                        public RelNode visit(LogicalSort sort) {
                            return super.visit(sort);
                        }

                        @Override
                        public RelNode visit(TableScan scan) {
                            MycatLogicTable logicTable = scan.getTable().unwrap(MycatLogicTable.class);
                            DataNode backendTableInfo = null;
                            if (logicTable != null && each != null) {
                                TableHandler tableHandler = logicTable.logicTable();
                                backendTableInfo = each.get(tableHandler.getUniqueName());
                                MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(logicTable, backendTableInfo);
                                return LogicalTableScan.create(scan.getCluster(),
                                        RelOptTableImpl.create(scan.getTable().getRelOptSchema(),
                                                scan.getRowType(),
                                                mycatPhysicalTable,
                                                ImmutableList.of(backendTableInfo.getUniqueName())),
                                        ImmutableList.of()
                                );
                            }
                            return scan;
                        }
                    });
                    builderList.add(newNode);
                }
                ImmutableList<RelNode> relNodes = builderList.build();
                SqlString sqlString = MycatCalciteSupport.INSTANCE.convertToSql(LogicalUnion.create(relNodes, true), dialect, Collections.emptyMap(), update, params);
                resMapBuilder.put(targetName, sqlString);
            }
        }
        return resMapBuilder.build();
    }

//    public List<String> getTargets(List<Object> params) {
//        distribution.
//        if (this.distribution.isPhy() || this.distribution.isBroadCast()) {
//            DataNode dataNode = distribution.getDataNodes().iterator().next();
//            return ImmutableList.of(dataNode.getTargetName());
//        } else {
//            ImmutableList.Builder<String> builder = ImmutableList.builder();
//            for (DataNode dataNode : this.distribution.getDataNodes(params)) {
//                builder.add(dataNode.getTargetName());
//            }
//            return builder.build();
//        }
//    }

    public RelNode applyDataNode(DataNode dataNode) {
        return this.relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable mycatLogicTable = scan.getTable().unwrap(MycatLogicTable.class);
                if (mycatLogicTable != null) {
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
        Expression mycatViewStash = implementor.stash(this, RelNode.class);
        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getEnumerable", RelNode.class);
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
        Expression mycatViewStash = implementor.stash(this, RelNode.class);
        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", RelNode.class);
        final Expression expression2 = Expressions.call(root, getEnumerable, mycatViewStash);
        builder.add(toRows(physType, expression2, getRowType().getFieldCount()));
        return implementor.result(physType, builder.toBlock());
    }

    public Optional<RexNode> getConditions() {
        return Optional.ofNullable(conditions);
    }
}