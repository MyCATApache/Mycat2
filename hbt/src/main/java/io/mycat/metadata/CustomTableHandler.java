package io.mycat.metadata;

import org.apache.calcite.plan.RelOptCluster;

import java.util.function.Supplier;

public interface CustomTableHandler {

    Supplier<Number> nextSequence();

    void createPhysicalTables();

    void dropPhysicalTables();

    Long insert(Object[] row);

    void replace(Object[] original, Object[] now);

    QueryBuilder createQueryBuilder(RelOptCluster cluster);
}
