package io.mycat.mpp.plan;

import io.mycat.Wrapper;
import io.mycat.mpp.DataContext;

import java.util.Optional;

public abstract class QueryPlan implements Wrapper {
    private Optional<NodePlan> to;

    public QueryPlan() {
        this.to = Optional.empty();
    }

    public abstract RowType getType();

    public abstract Scanner scan(DataContext dataContext, long flags);


    public void setTo(NodePlan to) {
        this.to = Optional.ofNullable(to);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws Exception {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws Exception {
        return false;
    }
}