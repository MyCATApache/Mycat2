//package io.mycat.hbt3;
//
//import com.alibaba.fastsql.sql.ast.SQLExpr;
//import com.alibaba.fastsql.sql.ast.SQLName;
//import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
//import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExtPartition;
//import io.mycat.hbt4.ShardingInfo;
//import org.apache.calcite.rex.RexNode;
//
//public class MycatTableAdapter extends AbstractMycatTable {
//    public MycatTableAdapter(String schemaName, String createTableSql, DrdsConst drdsConst) {
//        super(schemaName, createTableSql, drdsConst);
//    }
//
//    @Override
//    public ShardingInfo computeShardingInfo(SQLMethodInvokeExpr dbPartitionBy,
//                                            SQLExpr dbPartitions,
//                                            SQLMethodInvokeExpr tablePartitionBy,
//                                            SQLExpr tablePartitions,
//                                            MySqlExtPartition exPartition,
//                                            SQLName storedBy,
//                                            SQLName distributeByType) {
//        return null;
//    }
//
//    @Override
//    public Distribution computeDataNode(RexNode condition) {
//        return null;
//    }
//
//    @Override
//    public Distribution computeDataNode() {
//        return null;
//    }
//}