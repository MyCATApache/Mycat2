package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;

import java.util.Collections;

public class EmptyPlan extends ColumnThroughPlan {
    public EmptyPlan(QueryPlan from) {
        super(from);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        return Scanner.of(Collections.emptyIterator());
    }
}