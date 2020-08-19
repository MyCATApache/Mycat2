//package io.mycat.calcite.table;
//
//import org.apache.calcite.plan.RelOptTable;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.schema.ScannableTable;
//import org.apache.calcite.schema.TransientTable;
//import org.apache.calcite.schema.TranslatableTable;
//
//import java.util.Collections;
//import java.util.List;
//
//public class MycatSQLTableScan extends SingeTargetSQLTable implements ScannableTable, TransientTable, TranslatableTable {
//    final RelDataType relDataType;
//    final String sql;
//    public final String targetName;
//
//    public MycatSQLTableScan( RelDataType relDataType,String targetName, String sql) {
//        this.relDataType = relDataType;
//        this.sql = sql;
//        this.targetName = targetName;
//    }
//
//    @Override
//    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//        return relDataType;
//    }
//
//    @Override
//    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
//        return new MycatTransientSQLTableScan(context.getCluster(), relOptTable, sql);
//    }
//
//    public String getTargetName() {
//        return targetName;
//    }
//
//    public String getSql() {
//        return sql;
//    }
//
//    @Override
//    public List<Object> params() {
//        return Collections.emptyList();
//    }
//
//}