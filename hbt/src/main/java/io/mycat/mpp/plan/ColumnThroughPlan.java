package io.mycat.mpp.plan;

public abstract class ColumnThroughPlan extends NodePlan {

    public ColumnThroughPlan(QueryPlan from) {
        super(from);
    }

    @Override
    public RowType getType() {
        return from.getType();
    }
}