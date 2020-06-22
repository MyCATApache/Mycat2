package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class CompilerImpl implements Compiler {
    @Override
    public Scalar compile(List<RexNode> nodes, RelDataType inputRowType) {
        return null;
    }

    @Override
    public RelDataType combinedRowType(List<RelNode> inputs) {
        return null;
    }

    @Override
    public Source source(RelNode rel, int ordinal) {
        return null;
    }

    @Override
    public Sink sink(RelNode rel) {
        return null;
    }

    @Override
    public void enumerable(RelNode rel, Enumerable<Row> rowEnumerable) {

    }

    @Override
    public DataContext getDataContext() {
        return null;
    }

    @Override
    public Context createContext() {
        return null;
    }
}