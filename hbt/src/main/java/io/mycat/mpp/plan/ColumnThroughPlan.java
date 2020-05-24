package io.mycat.mpp.plan;

public abstract class ColumnThroughPlan extends NodePlan {

    public ColumnThroughPlan(QueryPlan from) {
        super(from);
    }

    @Override
    public Type getColumns() {
        return from.getColumns();
    }
}