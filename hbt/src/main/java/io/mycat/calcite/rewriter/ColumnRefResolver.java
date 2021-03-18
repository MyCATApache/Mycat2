package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.jetbrains.annotations.NotNull;

import java.util.*;


public class ColumnRefResolver extends RelShuttleImpl {

    TableScan tableScan;
    Map<Integer, ColumnInfo> map = new HashMap<>();
    ArrayList<Pair<ColumnInfo, ColumnInfo>> pairs = new ArrayList<>();

    public ColumnRefResolver() {
    }

    public ColumnInfo getBottomColumnInfo(int index) {
        if (tableScan != null) {
            return new ColumnInfo(tableScan, index);
        }
        return map.get(index);
    }

    public Collection<ColumnInfo> getBottomColumnInfoList(int index) {
        ColumnInfo bottomColumnInfo = getBottomColumnInfo(index);
        HashSet<ColumnInfo> set = new HashSet<>();
        set.add(bottomColumnInfo);
        collect(bottomColumnInfo, set);
        return set;
    }

    private void collect(ColumnInfo find, HashSet<ColumnInfo> set) {
        int originalSize = set.size();
        HashSet<ColumnInfo> adds = new HashSet<>();
        for (Pair<ColumnInfo, ColumnInfo> pair : pairs) {
            if (pair.left.equals(find)) {
                set.add(pair.right);
                adds.add(pair.right);
            }
            if (pair.right.equals(find)) {
                set.add(pair.left);
                adds.add(pair.left);
            }
        }
        if (!(set.size() > originalSize)) {//no changed
            return;
        }
        for (ColumnInfo add : adds) {
            collect(add, set);
        }
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
        List<Integer> list = aggregate.getGroupSet().asList();

        ColumnRefResolver columnMapping2 = new ColumnRefResolver();
        aggregate.getInput().accept(columnMapping2);
        int index = 0;
        for (Integer integer : list) {
            ColumnInfo bottomColumnInfo = columnMapping2.getBottomColumnInfo(integer);
            map.put(index, bottomColumnInfo);
            index++;
        }
        return aggregate;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        tableScan = null;
        return super.visit(match);
    }

    @Override
    public RelNode visit(TableScan scan) {
        tableScan = scan;
        return scan;
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
        return super.visit(calc);
    }


    @Override
    public RelNode visit(LogicalProject project) {
        ColumnRefResolver columnMapping2 = new ColumnRefResolver();
        project.getInput().accept(columnMapping2);
        int index = 0;
        for (RexNode expr : project.getProjects()) {
            if (expr instanceof RexInputRef) {
                ColumnInfo bottomColumnInfo = columnMapping2.getBottomColumnInfo(((RexInputRef) expr).getIndex());
                map.put(index, bottomColumnInfo);
            }
            index++;
        }
        return project;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        ColumnRefResolver leftColumnMapping = new ColumnRefResolver();
        join.getInput(0).accept(leftColumnMapping);

        ColumnRefResolver rightColumnMapping = new ColumnRefResolver();
        join.getInput(1).accept(rightColumnMapping);

        int leftFieldCount = join.getInput(0).getRowType().getFieldCount();
        for (int i = 0; i < leftFieldCount; i++) {
            ColumnInfo bottomColumnInfo = leftColumnMapping.getBottomColumnInfo(i);
            map.put(i, bottomColumnInfo);
        }
        int rightFieldCount = join.getInput(1).getRowType().getFieldCount();
        for (int i = 0; i < rightFieldCount; i++) {
            ColumnInfo bottomColumnInfo = rightColumnMapping.getBottomColumnInfo(i);
            map.put(leftFieldCount + i, bottomColumnInfo);
        }

        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.isEqui()) {
            for (IntPair pair : joinInfo.pairs()) {
                Collection<ColumnInfo> leftBottomColumnInfo = leftColumnMapping.getBottomColumnInfoList(pair.source);
                Collection<ColumnInfo> rightBottomColumnInfo = rightColumnMapping.getBottomColumnInfoList(pair.target);
                for (ColumnInfo l : leftBottomColumnInfo) {
                    for (ColumnInfo r : rightBottomColumnInfo) {
                        pairs.add(Pair.of(l, r));
                        pairs.add(Pair.of(r, l));
                    }
                }
            }
        }
        return join;
    }


    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        ColumnRefResolver leftColumnMapping = new ColumnRefResolver();
        correlate.getInput(0).accept(leftColumnMapping);

        ColumnRefResolver rightColumnMapping = new ColumnRefResolver();
        correlate.getInput(1).accept(rightColumnMapping);

        int leftFieldCount = correlate.getInput(0).getRowType().getFieldCount();
        for (int i = 0; i < leftFieldCount; i++) {
            ColumnInfo bottomColumnInfo = leftColumnMapping.getBottomColumnInfo(i);
            map.put(i, bottomColumnInfo);
        }
        int rightFieldCount = correlate.getInput(1).getRowType().getFieldCount();
        for (int i = 0; i < rightFieldCount; i++) {
            ColumnInfo bottomColumnInfo = rightColumnMapping.getBottomColumnInfo(i);
            map.put(leftFieldCount + i, bottomColumnInfo);
        }
        return correlate;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return setOp(union);
    }

    @NotNull
    private SetOp setOp(SetOp union) {
        List<ColumnRefResolver> columnMapping2List = new ArrayList<>();
        for (RelNode input : union.getInputs()) {
            ColumnRefResolver columnMapping = new ColumnRefResolver();
            input.accept(columnMapping);
            columnMapping2List.add(columnMapping);
        }
        int fieldCount = union.getRowType().getFieldCount();
        int count = 0;
        for (; count < fieldCount; count++) {
            for (ColumnRefResolver columnMapping2 : columnMapping2List) {
                ColumnInfo bottomColumnInfo = columnMapping2.getBottomColumnInfo(count);
                if (bottomColumnInfo == null) {
                    break;
                }
            }
        }
        if (count == fieldCount) {
            ColumnRefResolver main = columnMapping2List.get(0);
            if (columnMapping2List.stream().allMatch(i -> i.equals(main))) {
                this.map.putAll(main.map);
                this.tableScan = (main.tableScan);
                this.pairs = main.pairs;
            }
        }

        return union;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return setOp(intersect);
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return setOp(minus);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnRefResolver that = (ColumnRefResolver) o;
        return Objects.equals(tableScan, that.tableScan) && Objects.equals(map, that.map) && Objects.equals(pairs, that.pairs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableScan, map, pairs);
    }
}
