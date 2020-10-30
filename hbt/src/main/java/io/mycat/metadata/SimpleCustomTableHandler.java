package io.mycat.metadata;

import org.apache.calcite.plan.RelOptCluster;

import java.util.function.Supplier;

public class SimpleCustomTableHandler implements CustomTableHandler {
    @Override
    public Supplier<String> nextSequence() {
        return null;
    }

    @Override
    public void createPhysicalTables() {

    }

    @Override
    public void dropPhysicalTables() {

    }

    @Override
    public Long insert(Object[] row) {
        return null;
    }

    @Override
    public void replace(Object[] original, Object[] now) {

    }

    @Override
    public QueryBuilder createQueryBuilder(RelOptCluster cluster) {
        return null;
    }
}
