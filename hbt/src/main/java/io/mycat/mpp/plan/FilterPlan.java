package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import io.mycat.mpp.SqlValue;

import java.util.Collections;
import java.util.List;

public class FilterPlan extends ColumnThroughPlan {
    final List<SqlValue> conds;

    public FilterPlan(QueryPlan from,List<SqlValue> conds) {
        super(from);
        this.conds = conds;
    }
    public static FilterPlan create(QueryPlan from,SqlValue cond){
        return create(from, Collections.singletonList(cond));
    }
    public static FilterPlan create(QueryPlan from,List<SqlValue> conds){
        return new FilterPlan(from,conds);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        Type columns = from.getColumns();
        return Scanner.of( from.scan(dataContext, flags).stream()
                .filter(dataAccessor -> conds.stream().
                        allMatch(c ->
                                c.getValueAsBoolean(columns,dataAccessor,dataContext))));
    }


}