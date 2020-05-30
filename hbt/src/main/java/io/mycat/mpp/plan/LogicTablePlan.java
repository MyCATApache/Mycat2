package io.mycat.mpp.plan;

import lombok.Getter;

@Getter
public abstract class LogicTablePlan extends QueryPlan {
    final String schemaName;
    final String tableName;
    private RowType rowType;

    public LogicTablePlan(String schemaName, String tableName, RowType rowType) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.rowType = rowType;
    }

    @Override
    public RowType getType() {
        return rowType;
    }
}