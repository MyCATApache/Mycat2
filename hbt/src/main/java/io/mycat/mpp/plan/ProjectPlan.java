package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;
import io.mycat.mpp.SqlValue;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProjectPlan extends NodePlan {
    final List<SqlValue> values;
    final List<String> aliasList;

    public ProjectPlan(QueryPlan from, List<SqlValue> values, List<String> aliasList) {
        super(Objects.requireNonNull(from));
        this.values = Objects.requireNonNull(values);
        this.aliasList = aliasList == null || aliasList.isEmpty() ? values.stream().map(i -> i.toParseTree().toString()).collect(Collectors.toList()) : aliasList;
        if (values.isEmpty()) {
            throw new AssertionError();
        }
    }

    public static ProjectPlan create(QueryPlan from, List<SqlValue> values, List<String> aliasList) {
        return new ProjectPlan(from, values, aliasList);
    }

    public static ProjectPlan create(QueryPlan out, List<SqlValue> input) {
        return create(out,input,null);
    }

    @Override
    public RowType getType() {
        return RowType.of(values,aliasList);
    }



    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        RowType columns = from.getType();
        return Scanner.of(from.scan(dataContext, flags).stream().map(dataAccessor -> {
            Object[] row = new Object[values.size()];
            int index = 0;
            for (SqlValue value : values) {
                row[index] = value.getValue(columns, dataAccessor, dataContext);
                ++index;
            }
            return dataAccessor.map(row);
        }));
    }

    private Object eval(RowType columns, SqlValue c, Object[] row) {
        return false;
    }
}