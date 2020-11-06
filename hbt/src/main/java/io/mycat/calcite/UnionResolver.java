//package io.mycat.calcite;
//
//import com.google.common.collect.ImmutableList;
//import io.mycat.calcite.table.MycatSQLTableScan;
//import io.mycat.calcite.table.StreamUnionTable;
//import org.apache.calcite.plan.RelOptTable;
//import org.apache.calcite.prepare.RelOptTableImpl;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.RelShuttleImpl;
//import org.apache.calcite.rel.logical.LogicalTableScan;
//import org.apache.calcite.rel.logical.LogicalUnion;
//import org.apache.calcite.tools.RelBuilder;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//
//public class UnionResolver extends RelShuttleImpl {
// public    boolean change = true;
//
//    @Override
//    public RelNode visit(LogicalUnion union) {
//        change = false;
////        boolean inTransaction = calciteDataContext.getUponDBContext().isInTransaction();
//
//        //calcite的默认union执行器的输入不能超过2个
//
//        RelBuilder relBuilder = MycatCalciteSupport.INSTANCE.relBuilderFactory.create(union.getCluster(), null);
//
//        ArrayList<RelNode> inputs = new ArrayList<>();
//        CalciteUtls.collect(union, inputs);
//        //收集union的输入节点
//
//
////        //union并行化
////        if (inputs.stream().allMatch(p -> p.getTable() != null && p.getTable().unwrap(MycatSQLTableScan.class) != null)) {
////            List<MycatSQLTableScan> relNodes = (List)
////                    (inputs.stream().map(p -> p.getTable().unwrap(MycatSQLTableScan.class)).collect(Collectors.toList()));
////            StreamUnionTable scanOperator = new StreamUnionTable(relNodes);
////            RelOptTable table = RelOptTableImpl.create(
////                    null,
////                    scanOperator.getRowType(MycatCalciteSupport.INSTANCE.TypeFactory),
////                    scanOperator,
////                    ImmutableList.of(union.toString()));
////            change = true;
////            return LogicalTableScan.create(union.getCluster(), table, ImmutableList.of());
////        }
//
//        //不能并行的转换为执行器能运行的形状
//        if (inputs.size() > 2) {
//            change = true;
//            return inputs.stream().reduce((relNode1, relNode2) -> {
//                relBuilder.clear();
//                relBuilder.push(relNode1);
//                relBuilder.push(relNode2);
//                return relBuilder.union(!union.isDistinct()).build();
//            }).get();
//        } else {
//            change = false;
//            return union;
//        }
//
//    }
//}