package io.mycat;


import java.util.List;
import java.util.function.Supplier;

public interface TableHandler {
//
//    public Function<MySqlInsertStatement, Iterable<ParameterizedValues>> insertHandler();
//
//    public Function<MySqlUpdateStatement, Iterable<TextUpdateInfo>> updateHandler();
//
//    public Function<MySqlDeleteStatement, Iterable<TextUpdateInfo>> deleteHandler();

    public LogicTableType getType();

    String getSchemaName();

    String getTableName();

    String getCreateTableSQL();

    List<SimpleColumnInfo> getColumns();

    SimpleColumnInfo getColumnByName(String name);

    SimpleColumnInfo getAutoIncrementColumn();

    String getUniqueName();

    Supplier<String> nextSequence();

    default boolean isAutoIncrement() {
        return getAutoIncrementColumn() != null;
    }

    void createPhysicalTables();

    void dropPhysicalTables();
}