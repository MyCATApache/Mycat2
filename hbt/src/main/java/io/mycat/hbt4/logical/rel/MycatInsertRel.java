package io.mycat.hbt4.logical.rel;

import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt4.*;
import io.mycat.router.ShardingTableHandler;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

@Getter
public class MycatInsertRel extends AbstractRelNode implements MycatRel {

    private static RelOptCluster cluster = DrdsRunner.newCluster();
    private final int finalAutoIncrementIndex;
    private final List<Integer> shardingKeys;
    private final MySqlInsertStatement mySqlInsertStatement;
    private final ShardingTableHandler logicTable;
    private final String[] columnNames;

    public static MycatInsertRel create( int finalAutoIncrementIndex,
                                         List<Integer> shardingKeys,
                                         MySqlInsertStatement mySqlInsertStatement,
                                         ShardingTableHandler logicTable) {
        return create(cluster,finalAutoIncrementIndex,shardingKeys,mySqlInsertStatement,logicTable);
    }
    public static MycatInsertRel create(RelOptCluster cluster,
                                        int finalAutoIncrementIndex,
                                        List<Integer> shardingKeys,
                                        MySqlInsertStatement mySqlInsertStatement,
                                        ShardingTableHandler logicTable) {
        return new MycatInsertRel(cluster,finalAutoIncrementIndex,shardingKeys,mySqlInsertStatement,logicTable);
    }
    protected MycatInsertRel(RelOptCluster cluster,
                             int finalAutoIncrementIndex,
                             List<Integer> shardingKeys,
                             MySqlInsertStatement mySqlInsertStatement,
                             ShardingTableHandler logicTable) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.finalAutoIncrementIndex = finalAutoIncrementIndex;
        this.shardingKeys = shardingKeys;
        this.mySqlInsertStatement = mySqlInsertStatement;
        this.logicTable = logicTable;
        List<SQLIdentifierExpr> columns = (List)mySqlInsertStatement.getColumns();
        this.columnNames = columns.stream().map(i -> i.normalizedName()).toArray(size -> new String[size]);

        this.rowType= RelOptUtil.createDmlRowType(
                SqlKind.INSERT, getCluster().getTypeFactory());
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatInsertRel").into();
//        for (ParameterizedValues value : values) {
//
//            String target = value.getTarget();
//            String sql = value.getSql();
//            List<Object> params = value.getParams();
//            writer.name("Values").item("targetName", target)
//                    .item("sql", sql)
//                    .item("params", params)
//                    .ret();
//        }

        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

}