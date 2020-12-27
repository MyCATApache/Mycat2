package io.mycat;


import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface TableHandler {

    public LogicTableType getType();

    String getSchemaName();

    String getTableName();

    String getCreateTableSQL();

    default List<SimpleColumnInfo> getPrimaryKeyList(){
        return getColumns().stream()
                .filter(SimpleColumnInfo::isPrimaryKey)
                .collect(Collectors.toList());
    }

    List<SimpleColumnInfo> getColumns();

    Map<String,IndexInfo> getIndexes();

    SimpleColumnInfo getColumnByName(String name);

    SimpleColumnInfo getAutoIncrementColumn();

    String getUniqueName();

    Supplier<Number> nextSequence();

    default boolean isAutoIncrement() {
        return getAutoIncrementColumn() != null;
    }

    void createPhysicalTables();

    void dropPhysicalTables();


}