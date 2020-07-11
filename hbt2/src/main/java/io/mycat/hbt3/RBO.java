package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.ShardingInfo;
import io.mycat.hbt4.physical.MergeSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.mapping.IntPair;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RBO extends RelShuttleImpl {
    final static NextConvertor nextConvertor = new NextConvertor();

    static {
        nextConvertor.put(TableScan.class,
                Project.class, Union.class, Aggregate.class, Sort.class, Filter.class, Join.class);
        nextConvertor.put(Filter.class,
                Project.class, Union.class, Aggregate.class, Sort.class);
        nextConvertor.put(Join.class,
                Project.class, Union.class, Aggregate.class, Filter.class, Join.class, Sort.class);
        nextConvertor.put(Project.class,
                Project.class, Union.class, Aggregate.class, Sort.class);
        nextConvertor.put(Aggregate.class,
                Project.class, Union.class, Filter.class, Sort.class);
    }

    public RBO() {
    }

    @Override
    public RelNode visit(TableScan scan) {
        return View.of(scan, scan.getTable().unwrap(MycatTable.class).computeDataNode());
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        return scan;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        RelNode input = filter.getInput().accept(this);
        if (nextConvertor.check(input, Filter.class)) {
            return filter(input, filter);
        } else {
            return filter.copy(filter.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return null;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RelNode input = project.getInput().accept(this);
        if (nextConvertor.check(input, Project.class)) {
            return project(input, project);
        } else {
            return project.copy(project.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        List<RelNode> inputs = join.getInputs();
        RelNode left = inputs.get(0).accept(this);
        RelNode right = inputs.get(1).accept(this);
        if (nextConvertor.check(left, Join.class) && (nextConvertor.check(right, Join.class))) {
            return join(left, right, join);
        } else {
            return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
        }
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        List<RelNode> inputs = correlate.getInputs();
        RelNode left = inputs.get(0).accept(this);
        RelNode right = inputs.get(1).accept(this);
        if (nextConvertor.check(left, Correlate.class) && (nextConvertor.check(right, Correlate.class))) {
            return correlate(left, right, correlate);
        } else {
            return correlate.copy(correlate.getTraitSet(), ImmutableList.of(left, right));
        }
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode i : union.getInputs()) {
            RelNode accept = i.accept(this);
            inputs.add(accept);
        }
        return union(inputs, union);
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return null;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        RelNode input = aggregate.getInput().accept(this);
        if (nextConvertor.check(input, Aggregate.class)) {
            return aggregate(input, aggregate);
        } else {
            return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return null;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        RelNode input = sort.getInput().accept(this);
        if (nextConvertor.check(input, Sort.class)) {
            return sort(input, sort);
        } else {
            return sort.copy(sort.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return null;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return null;
    }

    @Override
    public RelNode visit(RelNode other) {
        return null;
    }

    private static RelNode sort(RelNode input, LogicalSort sort) {
        PartInfo dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDataNode();
            input = ((View) input).getRelNode();
        }
        if (dataNodeInfo == null) {
            return sort.copy(input.getTraitSet(), ImmutableList.of(input));
        }
        int size = dataNodeInfo.size();
        if (size == 1) {
            input = sort.copy(input.getTraitSet(), ImmutableList.of(input));
            return View.of(input, dataNodeInfo);
        } else {
            RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
            RexNode offset = sort.offset == null ? rexBuilder.makeExactLiteral(BigDecimal.ZERO) : sort.offset;
            RexNode fetch = sort.fetch == null ? rexBuilder.makeExactLiteral(BigDecimal.valueOf(Long.MAX_VALUE)) : sort.fetch;
            input = LogicalSort.create(input, sort.getCollation()
                    , rexBuilder.makeExactLiteral(BigDecimal.ZERO)
                    , rexBuilder.makeCall(SqlStdOperatorTable.PLUS, offset, fetch));
            input = View.of(input, dataNodeInfo);
            return new MergeSort(
                    input.getCluster(),
                    input.getTraitSet().replace(MycatConvention.INSTANCE),
                    input,
                    sort.collation, offset, fetch);
        }
    }


    private RelNode aggregate(RelNode input, LogicalAggregate aggregate) {
        RelOptCluster cluster = input.getCluster();
        PartInfo dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDataNode();
            input = ((View) input).getRelNode();
        }
        if (dataNodeInfo == null) {
            input = aggregate.copy(input.getTraitSet(), ImmutableList.of(input));
            return input;
        }
        int size = dataNodeInfo.size();
        if (size == 1) {
            input = aggregate.copy(input.getTraitSet(), ImmutableList.of(input));
            return View.of(input, dataNodeInfo);
        } else {
            if (!(input instanceof Union)) {
                input = LogicalUnion.create(ImmutableList.of(input, input), true);
                input = aggregate.copy(input.getTraitSet(), ImmutableList.of(input));
            }
            HepProgramBuilder hepProgram = new HepProgramBuilder();
            hepProgram.addMatchLimit(1);
            hepProgram.addRuleInstance(AggregateUnionTransposeRule.INSTANCE);
            HepPlanner planner = new HepPlanner(hepProgram.build());
            planner.setRoot(input);
            RelNode bestExp = planner.findBestExp();
            MultiView multiView = new MultiView(cluster.traitSetOf(MycatConvention.INSTANCE),
                    bestExp.getInput(0).getInput(0),
                    dataNodeInfo);
            return aggregate.copy(input.getTraitSet(), ImmutableList.of(multiView));
        }
    }

    private static RelNode union(List<RelNode> inputs, LogicalUnion union) {
        return union.copy(union.getTraitSet(), inputs);
    }

    private static RelNode correlate(RelNode left, RelNode right, LogicalCorrelate correlate) {
        return correlate.copy(correlate.getTraitSet(), ImmutableList.of(left, right));
    }

    private RelNode join(RelNode left, RelNode right, LogicalJoin join) {
        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.isEqui()) {
            if (left instanceof View && right instanceof View) {
                if (((View) left).getRelNode() instanceof LogicalTableScan && ((View) right).getRelNode() instanceof LogicalTableScan) {
                    MycatTable m = ((View) left).getRelNode().getTable().unwrap(MycatTable.class);
                    MycatTable s = ((View) right).getRelNode().getTable().unwrap(MycatTable.class);
                    ShardingInfo l = m.getShardingInfo();
                    ShardingInfo r = s.getShardingInfo();
                    boolean canPush = (l.isGlobal() || r.isGlobal());
                    if (!canPush) {
                        if (Objects.deepEquals(l, r)) {
                            List<IntPair> pairs = joinInfo.pairs();
                            int size = pairs.size();
                            for (IntPair pair : pairs) {
                                String s1 = m.getRowType().getFieldNames().get(pair.source);
                                String s2 = s.getRowType().getFieldNames().get(pair.target);
                                if (l.getSchemaKeys().contains(s1) && r.getSchemaKeys().contains(s1)
                                        &&
                                        l.getTableKeys().contains(s1) && r.getTableKeys().contains(s2)) {
                                    size--;
                                }else {
                                    break;
                                }
                            }
                            if (size == 0) {
                                canPush = true;
                            }
                        }
                    }
                    if (canPush) {
                        return View.of(join.copy(join.getTraitSet(), ImmutableList.of(((View) left).getRelNode(), ((View) right).getRelNode())));
                    }
                }
            }
        }
        return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
    }

    private static RelNode filter(RelNode input, LogicalFilter filter) {
        PartInfo dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDataNode();
            input = ((View) input).getRelNode();
        }
        if (input instanceof LogicalTableScan) {
            RexNode condition = filter.getCondition();
            RelOptTable table = input.getTable();
            MycatTable sTable = table.unwrap(MycatTable.class);
            return View.of(filter.copy(input.getTraitSet(), ImmutableList.of(input)), sTable.computeDataNode(condition));
        }
        input = filter.copy(input.getTraitSet(), ImmutableList.of(input));
        return View.of(input, dataNodeInfo);
    }

    private static RelNode project(RelNode input, LogicalProject project) {
        PartInfo dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDataNode();
            input = ((View) input).getRelNode();
        }
        input = project.copy(project.getTraitSet(), ImmutableList.of(input));
        return View.of(input, dataNodeInfo);
    }

}