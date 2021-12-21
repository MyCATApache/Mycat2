package io.ordinate.engine.physicalplan;

import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.TableHandler;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatTableScan;
import io.mycat.prototypeserver.mysql.VisualTableHandler;
import io.ordinate.engine.factory.FactoryUtil;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptTable;

import java.util.Collections;
import java.util.List;

import static io.ordinate.engine.factory.FactoryUtil.toArrowSchema;

public class VisualTablePlanImpl implements PhysicalPlan {
    private MycatTableScan relNode;

    public VisualTablePlanImpl(MycatTableScan relNode) {

        this.relNode = relNode;
    }

    @Override
    public Schema schema() {
        return FactoryUtil.toArrowSchema(relNode.getMycatRelDataTypeByCalcite());
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.emptyList();
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        RelOptTable table = relNode.getRelNode().getTable();
        MycatLogicTable logicTable = table.unwrap(MycatLogicTable.class);
        TableHandler tableHandler = logicTable.getTable();
        VisualTableHandler visualTableHandler = (VisualTableHandler) tableHandler;
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        MycatRelDataType mycatRelDataTypeByCalcite = relNode.getMycatRelDataTypeByCalcite();
        Schema schema = toArrowSchema(mycatRelDataTypeByCalcite);
        return ValuesPlan.create(schema,visualTableHandler.scanAll().blockingIterable()).execute(rootContext);
    }

    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {
        System.out.println();
    }
}
