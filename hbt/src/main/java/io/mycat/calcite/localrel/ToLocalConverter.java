package io.mycat.calcite.localrel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

import java.util.stream.Collectors;

public class ToLocalConverter extends RelShuttleImpl {

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return LocalAggregate.create(aggregate, aggregate.getInput().accept(this));
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return super.visit(match);
    }

    @Override
    public RelNode visit(TableScan scan) {
        return LocalTableScan.create(scan);
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        return super.visit(scan);
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return LocalValues.create(values);
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return LocalFilter.create(filter,filter.getInput().accept(this));
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return LocalCalc.create(calc,calc.getInput().accept(this));
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return LocalProject.create(project,project.getInput().accept(this));
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return LocalJoin.create(join,join.getLeft().accept(this),join.getRight().accept(this));
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return LocalCorrelate.create(correlate,correlate.getLeft().accept(this),correlate.getRight().accept(this));
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return LocalUnion.create(union,union.getInputs().stream().map(i->i.accept(this)).collect(Collectors.toList()));
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
        return LocalSort.create(sort,sort.getInput().accept(this));
    }
}
