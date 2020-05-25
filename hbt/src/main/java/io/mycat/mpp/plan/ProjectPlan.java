package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import io.mycat.mpp.SqlValue;

import java.util.List;

public class ProjectPlan extends NodePlan {
    final List<SqlValue> values;

    public ProjectPlan(QueryPlan from, List<SqlValue> values) {
        super(from);
        this.values = values;
    }

    @Override
    public RowType getType() {
        return null;
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        RowType columns = from.getType();
        return Scanner.of(from.scan(dataContext, flags).stream().map(dataAccessor -> {
            Object[] row = new Object[values.size()];
            int index = 0;
            for (SqlValue value : values) {
                row[index] = value.getValue(columns,dataAccessor,dataContext);
                ++index;
            }
            return dataAccessor.map(row);
        }));
    }

    private Object eval(RowType columns, SqlValue c, Object[] row) {
        return false;
    }
}