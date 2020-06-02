package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import io.mycat.mpp.SqlValue;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class MockQueryPlan extends QueryPlan {
    final RowType rowType;

    @Override
    public RowType getType() {
        return rowType;
    }
    public static MockQueryPlan create(List<SqlValue> sqlValueList){
        return create(RowType.of(sqlValueList));
    }
    public static MockQueryPlan create(RowType rowType){
        return new MockQueryPlan(rowType);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        throw new UnsupportedOperationException();
    }
}