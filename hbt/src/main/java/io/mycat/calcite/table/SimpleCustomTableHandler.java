package io.mycat.calcite.table;

import org.apache.calcite.plan.RelOptCluster;

import java.util.*;
import java.util.function.Supplier;

public class SimpleCustomTableHandler implements CustomTableHandler {

    private final LogicTable logicTable;
    private final Map<String, Object> kvOptions;
    private final List<Object> listOptions;

    private Collection<Object[]> collection = new ArrayList<>();

    public SimpleCustomTableHandler(LogicTable logicTable,
                                    Map<String, Object> kvOptions,
                                    List<Object> listOptions) {
        this.logicTable = logicTable;
        this.kvOptions = kvOptions;
        this.listOptions = listOptions;
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
        collection.add(row);
        return null;
    }

    @Override
    public void replace(Object[] original, Object[] now) {
        for (Object[] e : collection) {
            if (Arrays.deepEquals(original, e)) {
                System.arraycopy(now, 0, e, 0, e.length);
                break;
            }
        }
    }

    @Override
    public QueryBuilder createQueryBuilder(RelOptCluster cluster) {
        return QueryBuilder.createDefaultQueryBuilder(cluster,
                logicTable.getUniqueName(),
                collection);
    }
}
