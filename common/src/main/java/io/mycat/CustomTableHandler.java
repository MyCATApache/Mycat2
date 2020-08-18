package io.mycat;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class CustomTableHandler implements TableHandler {

    @Override
    public LogicTableType getType() {
        return null;
    }

    @Override
    public String getSchemaName() {
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public String getCreateTableSQL() {
        return null;
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return null;
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return null;
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return null;
    }

    @Override
    public String getUniqueName() {
        return null;
    }

    @Override
    public Supplier<String> nextSequence() {
        return null;
    }
}