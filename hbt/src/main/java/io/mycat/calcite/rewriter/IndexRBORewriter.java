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

import io.mycat.calcite.logical.IndexTableView;
import io.mycat.calcite.table.AbstractMycatTable;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.router.ShardingTableHandler;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.mycat.util.CalciteUtls.unCastWrapper;

public class IndexRBORewriter<T> extends SQLRBORewriter {
    boolean apply = false;
    public IndexRBORewriter() {
        super();
    }

    @Override
    public RelNode visit(TableScan scan) {
        Optional<Iterable<Object[]>> indexTableView = checkIndex(scan);
        if (indexTableView.isPresent()) {
            Iterable<Object[]> t = indexTableView.get();
            apply = true;
            return new IndexTableView(scan, (Iterable<Object[]>) t);
        } else {
            return super.visit(scan);
        }
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        Optional<Iterable<Object[]>> optional = checkIndex(filter);
        if (optional.isPresent()) {
            apply = true;
            IndexTableView indexTableView = new IndexTableView(filter.getInput(), (Iterable<Object[]>) optional.get());
            return filter.copy(filter.getTraitSet(), indexTableView, filter.getCondition());
        } else {
            return super.visit(filter);
        }
    }

    @Override
    public RelNode visit(LogicalProject project) {
        Optional<Iterable<Object[]>> optional = checkIndex(project);
        if (optional.isPresent()) {
            apply = true;
            IndexTableView indexTableView = new IndexTableView(project.getInput(), (Iterable<Object[]>) optional.get());
            return project.copy(project.getTraitSet(),
                    indexTableView, project.getProjects(),
                    project.getRowType());
        } else {
            return super.visit(project);
        }
    }


    public Optional<Iterable<Object[]>> checkIndex(RelNode input) {
        if (checkTable(input)) {
            LogicalTableScan logicalTableScan = (LogicalTableScan) input;
            if (!isSharding(logicalTableScan)) {
                return Optional.empty();
            }
            RelOptTable table = logicalTableScan.getTable();
            MycatLogicTable mycatLogicTable = table.unwrap(MycatLogicTable.class);
            ShardingTableHandler shardingTableHandler = (ShardingTableHandler) mycatLogicTable.getTable();
            return (Optional<Iterable<Object[]>>) shardingTableHandler.canIndexTableScan();
        }
        if (checkProjectTable(input)) {
            assert input instanceof LogicalProject;
            LogicalProject project = (LogicalProject) input;
            LogicalTableScan tableScan = (LogicalTableScan) project.getInput();
            if (!isSharding(tableScan)) {
                return Optional.empty();
            }
            RelOptTable table = tableScan.getTable();
            MycatLogicTable mycatLogicTable = table.unwrap(MycatLogicTable.class);
            ShardingTableHandler shardingTableHandler = (ShardingTableHandler) mycatLogicTable.getTable();
            return (Optional<Iterable<Object[]>>) shardingTableHandler.canIndexTableScan(map2IntArray(project));
        }
        if (checkProjectFilterTable(input)) {
            assert input instanceof LogicalProject;
            LogicalProject project = (LogicalProject) input;
            LogicalFilter filter = (LogicalFilter) project.getInput();
            LogicalTableScan tableScan = (LogicalTableScan) filter.getInput();
            if (!isSharding(tableScan)) {
                return Optional.empty();
            }
            RelOptTable table = tableScan.getTable();
            MycatLogicTable mycatLogicTable = table.unwrap(MycatLogicTable.class);
            ShardingTableHandler shardingTableHandler = (ShardingTableHandler) mycatLogicTable.getTable();
            List<RexNode> rexNodes = (List<RexNode>) Collections.singletonList(
                    filter.getCondition());
            if (rexNodes.size() == 1) {
                RexNode rexNode = rexNodes.get(0);
                if (rexNode.getKind() == SqlKind.EQUALS) {
                    RexCall rexNode1 = (RexCall) rexNode;
                    List<RexNode> operands = rexNode1.getOperands();
                    RexNode left = operands.get(0);
                    left = unCastWrapper(left);
                    RexNode right = operands.get(1);
                    right = unCastWrapper(right);
                    int index = ((RexInputRef) left).getIndex();
                    Object value = null;
                    if (right instanceof RexLiteral){
                        value = ((RexLiteral) right).getValue2();
                    }else if (right instanceof RexDynamicParam){
//                        value = super.params.get (((RexDynamicParam)right).getIndex());
                    }else {
                        return Optional.empty();
                    }

                    return (Optional<Iterable<Object[]>>) shardingTableHandler.canIndexTableScan(map2IntArray(project),
                            new int[]{index}, new Object[]{value});
                }
            }
        }
        return Optional.empty();
    }

    private static int[] map2IntArray(LogicalProject project) {
        final List<Integer> selectedColumns = new ArrayList<>();
        final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            public Void visitInputRef(RexInputRef inputRef) {
                if (!selectedColumns.contains(inputRef.getIndex())) {
                    selectedColumns.add(inputRef.getIndex());
                }
                return null;
            }
        };
        visitor.visitEach(project.getProjects());
        return ImmutableIntList.copyOf(selectedColumns).toIntArray();
    }

    private static boolean isSharding(LogicalTableScan tableScan) {
        RelOptTable table = tableScan.getTable();
        AbstractMycatTable abstractMycatTable = table.unwrap(AbstractMycatTable.class);
        return abstractMycatTable.isSharding();
    }

    private static boolean checkTable(RelNode input) {
        return input instanceof LogicalTableScan;
    }

    private static boolean checkProjectTable(RelNode input) {
        return input instanceof LogicalProject
                &&
                checkTable(((LogicalProject) input).getInput());
    }

    private static boolean checkProjectFilterTable(RelNode input) {
        return input instanceof LogicalProject
                &&
                ((LogicalProject) input).getInput() instanceof LogicalFilter
                &&
                checkTable(((LogicalFilter) ((LogicalProject) input).getInput()).getInput());
    }

    public boolean isApply() {
        return apply;
    }
}
