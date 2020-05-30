package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MockQueryPlan extends QueryPlan {
    final RowType rowType;

    @Override
    public RowType getType() {
        return rowType;
    }

    public static MockQueryPlan create(RowType rowType){
        return new MockQueryPlan(rowType);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        throw new UnsupportedOperationException();
    }
}