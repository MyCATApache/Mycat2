package io.mycat.calcite.relBuilder;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;

public class IsSameTarget extends RelVisitor {
    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        super.visit(node, ordinal, parent);
    }
}