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
package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.Map;

public class ColumnMapping extends RelShuttleImpl {

    TableScan tableScan;
    Map<Integer, Integer> map;
    private final RelNode root;

    public ColumnMapping() {
        this(null);
    }

    public ColumnMapping(RelNode relNode) {
        this.root = relNode;
    }

    public ColumnInfo getBottomColumnInfo(int index) {
        RelDataType rowType = this.root.getRowType();
        this.root.accept(this);
        return null;
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
        updateMapping(calc.getProgram().getPartialMapping(calc.getInput().getRowType().getFieldCount()));
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
