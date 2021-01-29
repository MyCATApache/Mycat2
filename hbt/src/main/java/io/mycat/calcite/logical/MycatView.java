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
import io.mycat.DataNode;
import io.mycat.calcite.*;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.BuiltInMethod;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class MycatView extends AbstractRelNode implements MycatRel {
    RelNode relNode;
    Distribution distribution;

    public MycatView(RelTraitSet relTrait, RelNode input, Distribution dataNode) {
        super(input.getCluster(), relTrait);
        this.distribution = Objects.requireNonNull(dataNode);
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
    }
//    public View(RelTraitSet relTrait, List<RelNode> inputs, Distribution dataNode, boolean gather) {
//        super(inputs.get(0).getCluster(),relTrait);
//        this.distribution = Objects.requireNonNull(dataNode);
//        this.rowType = inputs.get(0).getRowType();
//        this.traitSet = relTrait;
//        this.gather = gather;
//    }
//    public static View of(List<RelNode> input, Distribution dataNodeInfo) {
//        return new View(input.get(0).getTraitSet().replace(MycatConvention.INSTANCE),input,dataNodeInfo,false);
//    }

    public static MycatView of(RelNode input, Distribution dataNodeInfo) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo);
    }

    public static MycatView of(RelTraitSet relTrait, RelNode input, Distribution dataNodeInfo) {
        return new MycatView(relTrait.replace(MycatConvention.INSTANCE), input, dataNodeInfo);
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
        return writer;
    }

    @NotNull
    private RelWriter innerExplainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        writer.item("\nrelNode", getSql(MycatSqlDialect.DEFAULT));
        String msg = StreamSupport
                .stream(distribution.getDataNodes().spliterator(), false)
                .map(i -> i.toString()).collect(Collectors.joining(",\n"));
        writer.item("\ndistribution", "\n" + msg);
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
        RelOptCost relOptCost = super.computeSelfCost(planner, mq);
        return relOptCost;
    }


    public RelNode expandToPhyRelNode() {
        if (this.distribution.isPhy() || this.distribution.isBroadCast()) {
            DataNode dataNode = distribution.getDataNodes().iterator().next();
            return applyDataNode(dataNode);
        } else {
            ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
            for (DataNode dataNode : this.distribution.getDataNodes()) {
                builder.add(applyDataNode(dataNode));
            }
            ImmutableList<RelNode> views = builder.build();
            return LogicalUnion.create(views, true);
        }
    }

//    public Map<String, List<View>> expandToPhyView() {
//        if (this.distribution.isSingle() || this.distribution.isBroadCast()) {
//            DataNode dataNode = distribution.getDataNodes().iterator().next();
//            return ImmutableMap.of(dataNode.getTargetName(),
//                    ImmutableList.of(View.of(applyDataNode(dataNode),
//                    Distribution.of(ImmutableList.of(dataNode), "")))
//            );
//        } else {
//            ImmutableMap.Builder<String,List<View>> builder = ImmutableMap.builder();
//            for (DataNode dataNode : this.distribution.getDataNodes()) {
//                builder.add(applyDataNode(dataNode));
//            }
//            ImmutableList<RelNode> views = builder.build();
//            return LogicalUnion.create(views, true);
//        }
//    }

    public ImmutableMultimap<String, SqlString> expandToSql(boolean update, List<Object> params) {
        if (this.distribution.isPhy() || this.distribution.isBroadCast()) {
            DataNode dataNode = distribution.getDataNodes().iterator().next();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(dataNode.getTargetName());
            SqlString sql = MycatCalciteSupport.INSTANCE.convertToSql(applyDataNode(dataNode), dialect, update, params);
            return ImmutableMultimap.of(dataNode.getTargetName(), sql);
        } else {
            ImmutableMultimap.Builder<String, SqlString> builder = ImmutableMultimap.builder();
            for (DataNode dataNode : this.distribution.getDataNodes(params)) {
                SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(dataNode.getTargetName());
                SqlString sql = MycatCalciteSupport.INSTANCE.convertToSql(applyDataNode(dataNode), dialect, update, params);
                builder.put(dataNode.getTargetName(), sql);
            }
            return builder.build();
        }
    }

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
        implementor.collectLeafRelNode(this);
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

    public static Expression toRows(PhysType physType, Expression expression,final int fieldCount ) {
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
        implementor.collectLeafRelNode(this);
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
        builder.add(toRows(physType, expression2,getRowType().getFieldCount()));
        return implementor.result(physType, builder.toBlock());
    }
}