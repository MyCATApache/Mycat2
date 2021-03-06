package io.mycat.calcite.physical;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.DrdsRunner;
import io.mycat.calcite.*;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.FastSqlUtils;
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

    public MySqlInsertStatement getMySqlInsertStatement() {
        return FastSqlUtils.clone(mySqlInsertStatement);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatInsertRel").into();
        return writer.ret();
    }

}