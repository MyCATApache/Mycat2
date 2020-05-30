package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;

import java.util.Collections;

public class DualPlan extends LogicTablePlan {

    public DualPlan() {
        super("metaData","dual",RowType.of());
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