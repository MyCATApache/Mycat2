package io.mycat.mpp.plan;

import io.mycat.mpp.AccessDataExpr;
import io.mycat.mpp.DataContext;

import java.util.Comparator;

public class OrderPlan extends ColumnThroughPlan {
    private final int[] fields;
    private final boolean[] directions;

    public static OrderPlan create(QueryPlan from,int[] fields,boolean[] directions){
        return new OrderPlan(from,fields,directions);
    }

    public OrderPlan(QueryPlan from, int[] fields, boolean[] directions) {
        super(from);
        this.fields = fields;
        this.directions = directions;
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        Type columns = from.getColumns();
        Comparator<DataAccessor> comparator = null;
        int i = 0;
        for (int field : fields) {
            Comparator<DataAccessor> columnsComparator = columns.getComparator(field);
            columnsComparator = directions[i] ? columnsComparator : columnsComparator.reversed();
            if (comparator == null) {
                comparator = columnsComparator;
            } else {
                comparator.thenComparing(columnsComparator);
            }
        }
        return Scanner.of(from.scan(dataContext, flags).stream().sorted(comparator).iterator());
    }
}