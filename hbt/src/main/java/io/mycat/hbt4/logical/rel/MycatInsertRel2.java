//package io.mycat.hbt4.logical.rel;
//
//import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
//import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
//import io.mycat.hbt3.DrdsRunner;
//import io.mycat.hbt4.*;
//import io.mycat.router.ShardingTableHandler;
//import lombok.Getter;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.plan.RelOptUtil;
//import org.apache.calcite.rel.AbstractRelNode;
//import org.apache.calcite.sql.SqlKind;
//
//import java.util.List;
///*
//
//    private MycatRel compileInsert(ShardingTableHandler logicTable,
//                                   MycatDataContext dataContext,
//                                   DrdsSql drdsSql,
//                                   OptimizationContext optimizationContext) {
//        MySqlInsertStatement mySqlInsertStatement = drdsSql.getSqlStatement();
//        List<SQLIdentifierExpr> columnsTmp = (List) mySqlInsertStatement.getColumns();
//        boolean autoIncrement = logicTable.isAutoIncrement();
//        int autoIncrementIndexTmp = -1;
//        ArrayList<Integer> shardingKeys = new ArrayList<>();
//        CustomRuleFunction function = logicTable.function();
//        List<SimpleColumnInfo> metaColumns;
//        if (columnsTmp.isEmpty()) {//fill columns
//            int index = 0;
//            for (SimpleColumnInfo column : metaColumns = logicTable.getColumns()) {
//                if (autoIncrement && logicTable.getAutoIncrementColumn() == column) {
//                    autoIncrementIndexTmp = index;
//                }
//                if (function.isShardingKey(column.getColumnName())) {
//                    shardingKeys.add(index);
//                }
//                mySqlInsertStatement.addColumn(new SQLIdentifierExpr(column.getColumnName()));
//                index++;
//            }
//        } else {
//            int index = 0;
//            metaColumns = new ArrayList<>();
//            for (SQLIdentifierExpr column : columnsTmp) {
//                SimpleColumnInfo simpleColumnInfo = logicTable.getColumnByName(SQLUtils.normalize(column.getName()));
//                metaColumns.add(simpleColumnInfo);
//                if (autoIncrement && logicTable.getAutoIncrementColumn() == simpleColumnInfo) {
//                    autoIncrementIndexTmp = index;
//                }
//                if (function.isShardingKey(simpleColumnInfo.getColumnName())) {
//                    shardingKeys.add(index);
//                }
//                index++;
//            }
//            if (autoIncrement && autoIncrementIndexTmp == -1) {
//                SimpleColumnInfo autoIncrementColumn = logicTable.getAutoIncrementColumn();
//                if (function.isShardingKey(autoIncrementColumn.getColumnName())) {
//                    shardingKeys.add(index);
//                }
//                metaColumns.add(autoIncrementColumn);
//                mySqlInsertStatement.addColumn(new SQLIdentifierExpr(autoIncrementColumn.getColumnName()));
//                SQLVariantRefExpr sqlVariantRefExpr = new SQLVariantRefExpr("?");
//                sqlVariantRefExpr.setIndex(-1);
//                for (SQLInsertStatement.ValuesClause valuesClause : mySqlInsertStatement.getValuesList()) {
//                    valuesClause.addValue(sqlVariantRefExpr);
//                }
//            }
//        }
//        final int finalAutoIncrementIndex = autoIncrementIndexTmp;
//        MycatInsertRel mycatInsertRel = MycatInsertRel.create(finalAutoIncrementIndex, shardingKeys, mySqlInsertStatement, logicTable);
//        optimizationContext.saveParameterized(drdsSql.getParameterizedString(), mycatInsertRel);
//        return mycatInsertRel;
//    }
//
//
//
//
//
//
// */
//@Getter
//public class MycatInsertRel2 extends AbstractRelNode implements MycatRel {
//
//    private static RelOptCluster cluster = DrdsRunner.newCluster();
//    private final int finalAutoIncrementIndex;
//    private final List<Integer> shardingKeys;
//    private final MySqlInsertStatement mySqlInsertStatement;
//    private final ShardingTableHandler logicTable;
//    private final String[] columnNames;
//
//    public static MycatInsertRel2 create(int finalAutoIncrementIndex,
//                                         List<Integer> shardingKeys,
//                                         MySqlInsertStatement mySqlInsertStatement,
//                                         ShardingTableHandler logicTable) {
//        return create(cluster,finalAutoIncrementIndex,shardingKeys,mySqlInsertStatement,logicTable);
//    }
//    public static MycatInsertRel2 create(RelOptCluster cluster,
//                                         int finalAutoIncrementIndex,
//                                         List<Integer> shardingKeys,
//                                         MySqlInsertStatement mySqlInsertStatement,
//                                         ShardingTableHandler logicTable) {
//        return new MycatInsertRel2(cluster,finalAutoIncrementIndex,shardingKeys,mySqlInsertStatement,logicTable);
//    }
//    protected MycatInsertRel2(RelOptCluster cluster,
//                              int finalAutoIncrementIndex,
//                              List<Integer> shardingKeys,
//                              MySqlInsertStatement mySqlInsertStatement,
//                              ShardingTableHandler logicTable) {
//        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
//        this.finalAutoIncrementIndex = finalAutoIncrementIndex;
//        this.shardingKeys = shardingKeys;
//        this.mySqlInsertStatement = mySqlInsertStatement;
//        this.logicTable = logicTable;
//        List<SQLIdentifierExpr> columns = (List)mySqlInsertStatement.getColumns();
//        this.columnNames = columns.stream().map(i -> i.normalizedName()).toArray(size -> new String[size]);
//
//        this.rowType= RelOptUtil.createDmlRowType(
//                SqlKind.INSERT, getCluster().getTypeFactory());
//    }
//
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        writer.name("MycatInsertRel").into();
////        for (ParameterizedValues value : values) {
////
////            String target = value.getTarget();
////            String sql = value.getSql();
////            List<Object> params = value.getParams();
////            writer.name("Values").item("targetName", target)
////                    .item("sql", sql)
////                    .item("params", params)
////                    .ret();
////        }
//
//        return writer.ret();
//    }
//
//    @Override
//    public Executor implement(ExecutorImplementor implementor) {
//        return implementor.implement(this);
//    }
//
//}