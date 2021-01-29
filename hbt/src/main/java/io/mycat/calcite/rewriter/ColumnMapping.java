package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.Map;

public class ColumnMapping extends RelShuttleImpl {

    TableScan tableScan;
    Map<Integer, Integer> map;

    public ColumnMapping() {

    }

    public boolean hasRes() {
        return tableScan != null && map != null;
    }

    public int mapping(int index) {
            return map.get(index);
    }

    private void updateMapping(Mappings.TargetMapping permutation) {
        if (tableScan != null && permutation != null) {
            ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
            for (IntPair intPair : permutation) {
                builder.put(intPair.target, intPair.source);
            }
            map = (builder.build());
        }
    }

    private void reset() {
        map = null;
        tableScan = null;
    }


    public static ImmutableMap<Integer, Integer> identity(TableScan scan) {
        ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
        for (Integer integer : scan.identity()) {
            builder.put(integer, integer);
        }
        return builder.build();
    }


    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        RelNode visit = super.visit(aggregate);
        map.clear();
        return visit;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        tableScan = null;
        return super.visit(match);
    }

    @Override
    public RelNode visit(TableScan scan) {
        tableScan = scan;

        map = identity(scan);
        return tableScan;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        tableScan = null;
        return super.visit(values);
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return super.visit(filter);
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        RelNode visit = super.visit(calc);
        updateMapping(calc.getProgram().getPermutation());
        return visit;
    }


    @Override
    public RelNode visit(LogicalProject project) {
        RelNode visit = super.visit(project);
        updateMapping(project.getMapping());
        return visit;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        RelNode relNode = super.visit(join);
        reset();
        return relNode;
    }


    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return super.visit(correlate);
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return super.visit(union);
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return super.visit(intersect);
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return super.visit(minus);
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return super.visit(sort);
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return super.visit(exchange);
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return super.visit(modify);
    }

}
