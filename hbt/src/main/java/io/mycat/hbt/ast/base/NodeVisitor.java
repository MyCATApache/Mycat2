package io.mycat.hbt.ast.base;

import io.mycat.hbt.ast.AggregateCall;
import io.mycat.hbt.ast.modify.ModifyTable;
import io.mycat.hbt.ast.query.*;

public interface NodeVisitor {
    void visit(MapSchema mapSchema);

    void visit(GroupSchema groupSchema);

    void visit(LimitSchema limitSchema);

    void visit(FromSchema fromSchema);

    void visit(SetOpSchema setOpSchema);

    void visit(FieldType fieldSchema);

    void visit(Literal literal);

    void visit(OrderSchema orderSchema);

    void visit(Identifier identifier);

    void visit(Expr expr);

    void visit(ValuesSchema valuesSchema);

    void visit(JoinSchema corJoinSchema);

    void visit(AggregateCall aggregateCall);

    void visit(FilterSchema filterSchema);

    void visit(ModifyTable modifyTable);

    void visit(DistinctSchema distinctSchema);

    void visit(ProjectSchema projectSchema);

    void visit(CorrelateSchema correlate);
}