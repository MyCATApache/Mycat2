package io.mycat.mpp.plan;

import io.mycat.mpp.DataContext;

import java.util.List;

public class ListReadOnlyTable extends LogicTablePlan {
    private final List<Object[]> list;

    public ListReadOnlyTable(String schemaName, String tableName, RowType rowType, List<Object[]> list) {
        super(schemaName, tableName, rowType);
        this.list = list;
    }

    public static ListReadOnlyTable create(String schemaName, String tableName, RowType rowType, List<Object[]> list) {
        return new ListReadOnlyTable(schemaName, tableName, rowType, list);
    }

    @Override
    public Scanner scan(DataContext dataContext, long flags) {
        return Scanner.of(list.stream().map(DataAccessor::of));
    }
}