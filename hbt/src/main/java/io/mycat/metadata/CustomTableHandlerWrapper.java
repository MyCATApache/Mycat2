package io.mycat.metadata;

import io.mycat.*;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Supplier;

public class CustomTableHandlerWrapper implements TableHandler {
    private final LogicTable logicTable;
    private final CustomTableHandler inner;


    @SneakyThrows
    public CustomTableHandlerWrapper(LogicTable logicTable,
                                     String klazz,
                                     Map<String, Object> kvOptions,
                                     List<Object> listOptions) {
        this.logicTable = logicTable;
        Class<?> aClass = Class.forName(klazz);
        Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(LogicTable.class,Map.class,List.class);
        this.inner = (CustomTableHandler) declaredConstructor.newInstance(logicTable, kvOptions, listOptions);
    }

    @Override
    public LogicTableType getType() {
        return logicTable.getType();
    }

    @Override
    public String getSchemaName() {
        return logicTable.getSchemaName();
    }

    @Override
    public String getTableName() {
        return logicTable.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return logicTable.getCreateTableSQL();
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return logicTable.getRawColumns();
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return logicTable.getColumnByName(name);
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return logicTable.getAutoIncrementColumn();
    }

    @Override
    public String getUniqueName() {
        return logicTable.getUniqueName();
    }

    @Override
    @SneakyThrows
    public Supplier<Number> nextSequence() {
        return this.inner.nextSequence();
    }

    @Override
    @SneakyThrows
    public void createPhysicalTables() {
        this.inner.createPhysicalTables();
    }

    @Override
    @SneakyThrows
    public void dropPhysicalTables() {
        this.inner.dropPhysicalTables();
    }

    @SneakyThrows
    public long insert(Object[] row) {
        return (Long) this.inner.insert(row);
    }

    @SneakyThrows
    public void replace(Object[] original, Object[] now) {
        this.inner.replace(original,now);
    }

    @SneakyThrows
    public QueryBuilder createQueryBuilder(RelOptCluster cluster) {
        QueryBuilder queryBuilder = this.inner.createQueryBuilder(cluster);
        return queryBuilder;
    }


}