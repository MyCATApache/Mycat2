//package io.mycat.hbt3;
//
//import com.google.common.collect.ImmutableList;
//import io.mycat.DataNode;
//import org.apache.calcite.linq4j.function.Experimental;
//import org.apache.calcite.plan.*;
//import org.apache.calcite.prepare.RelOptTableImpl;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.logical.LogicalFilter;
//import org.apache.calcite.rel.logical.LogicalTableScan;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.schema.TransientTable;
//import org.apache.calcite.schema.impl.ListTransientTable;
//import org.apache.calcite.tools.RelBuilder;
//
//public class MycatExpandPhyTableViewRule extends RelOptRule {
//    public final static MycatExpandPhyTableViewRule INSTANCE = new MycatExpandPhyTableViewRule();
//    public MycatExpandPhyTableViewRule() {
//        super(operand(LogicalFilter.class,operand(LogicalTableScan.class,none())),
//                "MycatExpandPhyTableViewRule");
//    }
//
//    @Override
//    public void onMatch(RelOptRuleCall call) {
//        RelBuilder builder = call.builder();
//        RelOptCluster cluster = builder.getCluster();
//        LogicalFilter filter = call.rel(0);
//        LogicalTableScan tableScan = call.rel(1);
//        RexNode condition = filter.getCondition();
//        MycatTable table = tableScan.getTable().unwrap(MycatTable.class);
//        if (table!=null){
//            Distribution distribution = table.computeDataNode(condition);
//            ImmutableList.Builder<RelNode> listBuilder = ImmutableList.builder();
//            builder.getRelOptSchema();
//            for (DataNode dataNode : distribution.dataNodes()) {
//                MycatTransientTable.create(dataNode,)
//                listBuilder.add(  filter.copy(filter.getTraitSet(),Vir,condition));
//            }
//            builder.un
//
//            call.transformTo( );
//        }
//    }
//    @Experimental
//    public RelBuilder transientScan(RelOptCluster cluster,DataNode dataNode, RelDataType rowType) {
//        TransientTable transientTable = new MycatTransientTable(dataNode, rowType);
//        RelOptTable relOptTable = RelOptTableImpl.create(
//                (RelOptSchema) cluster.ge,
//                rowType,
//                transientTable,
//                ImmutableList .of(dataNode.getTargetSchemaTable()));
//        RelNode scan =
//                struct.scanFactory.createScan(ViewExpanders.toRelContext(viewExpander, cluster), relOptTable);
//        push(scan);
//        rename(rowType.getFieldNames());
//        return this;
//    }
//}