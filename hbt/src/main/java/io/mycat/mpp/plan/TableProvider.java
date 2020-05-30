package io.mycat.mpp.plan;

public interface TableProvider {
    public QueryPlan create(String schemaName, String tableName) ;

    /**
     * {
     *         return ListReadOnlyTable.create(schemaName,
     *                 tableName,RowType.of(Column.of("1", Type.of(Type.INT,true))), );
     *     }
     * }
     */
}