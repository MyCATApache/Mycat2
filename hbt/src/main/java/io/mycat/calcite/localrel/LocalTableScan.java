package io.mycat.calcite.localrel;

import com.google.common.collect.ImmutableList;
import io.mycat.TableHandler;
import io.mycat.beans.mycat.MycatField;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.table.AbstractMycatTable;
import io.mycat.calcite.table.MycatLogicTable;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalTableScan extends TableScan implements LocalRel{
    public LocalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet.replace(LocalConvention.INSTANCE), hints, table);
    }
    public LocalTableScan(RelInput input) {
        this(input.getCluster(), input.getTraitSet(),input.getHints(), input.getTable("table"));
    }


    public static LocalTableScan create(TableScan tableScan) {
        return new LocalTableScan(tableScan.getCluster(), tableScan.getTraitSet(), tableScan.getHints(),tableScan.getTable());
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeCost(table.getRowCount(),0,0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        writer.item("hints",getHints());
        return writer;
    }

    public static final RelFactories.TableScanFactory TABLE_SCAN_FACTORY =
            (toRelContext, table) -> {
               return new LocalTableScan(toRelContext.getCluster(),toRelContext.getCluster().traitSet(), toRelContext.getTableHints(),table);
            };

    @Override
    public MycatRelDataType getMycatRelDataType() {
        MycatLogicTable mycatTable = getTable().unwrap(MycatLogicTable.class);
        TableHandler tableTable = mycatTable.getTable();
        List<MycatField> mycatFields = tableTable.getColumns().stream().map(c -> c.toMycatField()).collect(Collectors.toList());
        return MycatRelDataType.of(mycatFields);
    }
}
