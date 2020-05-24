package io.mycat.mpp.plan;

public abstract class NodePlan extends QueryPlan{
    final QueryPlan from;
    public NodePlan(QueryPlan from) {
        this.from = from;
        from.setTo(this);
    }
}