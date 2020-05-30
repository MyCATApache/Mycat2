package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;

public class LimitPlan extends ColumnThroughPlan {
   final long offset;
    final long count;
    public LimitPlan(QueryPlan from, long offset, long count) {
        super(from);
        this.offset = offset;
        this.count = count;
    }

    public static LimitPlan create(QueryPlan queryPlan, long offset, long count) {
        return new LimitPlan(queryPlan,offset,count);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        Scanner scan = from.scan(dataContext, flags);
        return Scanner.of(scan.stream().skip(offset).limit(count));
    }
}