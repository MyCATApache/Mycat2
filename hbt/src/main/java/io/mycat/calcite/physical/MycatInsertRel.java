/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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

    private final static RelOptCluster cluster = DrdsRunner.newCluster();
    private final int finalAutoIncrementIndex;
    private final List<Integer> shardingKeys;
    private final MySqlInsertStatement mySqlInsertStatement;
    private final ShardingTableHandler logicTable;
    private final String[] columnNames;

    public static MycatInsertRel create(
                                        int finalAutoIncrementIndex,
                                        List<Integer> shardingKeys,
                                        MySqlInsertStatement mySqlInsertStatement,
                                        ShardingTableHandler logicTable) {
        return new MycatInsertRel(finalAutoIncrementIndex,shardingKeys,mySqlInsertStatement,logicTable);
    }
    protected MycatInsertRel(
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