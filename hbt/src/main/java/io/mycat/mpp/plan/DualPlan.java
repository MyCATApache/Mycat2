package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;

import java.util.Collections;

public class DualPlan extends QueryPlan {

    public DualPlan() {
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        return Scanner.of(Collections.singletonList(DataAccessor.of(new Object[0])));
    }

    @Override
    public RowType getType() {
        return RowType.of();
    }
}