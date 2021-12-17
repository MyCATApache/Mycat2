package io.ordinate.engine.factory;

import io.mycat.TableHandler;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatTableScan;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.physicalplan.ValuesPlan;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptTable;

import static io.ordinate.engine.factory.FactoryUtil.toArrowSchema;

public class VisualTablescanFactory implements Factory {
    private MycatTableScan tableScan;

    public VisualTablescanFactory(MycatTableScan tableScan) {
        this.tableScan = tableScan;
    }

    @Override
    public PhysicalPlan create(ComplierContext context) {
        RelOptTable table = tableScan.getRelNode().getTable();
        MycatLogicTable logicTable = table.unwrap(MycatLogicTable.class);
        TableHandler tableHandler = logicTable.getTable();
        Observable<Object[]> tableObservable = context.getTableObservable(tableHandler.getSchemaName(), tableHandler.getTableName());
        MycatRelDataType mycatRelDataTypeByCalcite = tableScan.getMycatRelDataTypeByCalcite();
        Schema schema = toArrowSchema(mycatRelDataTypeByCalcite);
        return ValuesPlan.create(schema,tableObservable.blockingIterable());
    }


}
