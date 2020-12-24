package io.mycat.calcite.table;

import org.apache.calcite.plan.RelOptCluster;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class DualCustomTableHandler implements CustomTableHandler {
    private final LogicTable logicTable;
    private final Map map;
    private final List list;

    public DualCustomTableHandler(LogicTable logicTable,
                                  java.util.Map map,
                                  java.util.List list){

        this.logicTable = logicTable;
        this.map = map;
        this.list = list;
    }
    @Override
    public Supplier<Number> nextSequence() {
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
        return QueryBuilder.createDefaultQueryBuilder(cluster,"mycat.dual",
                Collections.singletonList(new Object[]{}));
    }
}
