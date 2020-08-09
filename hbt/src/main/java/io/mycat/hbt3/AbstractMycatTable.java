/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt3;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExtPartition;
import io.mycat.calcite.CalciteConvertors;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.ShardingInfo;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;

@Getter
public abstract class AbstractMycatTable extends AbstractTable {
    protected final String createTableSql;
    protected final RelDataType relDataType;
    protected final String schemaName;
    protected final String tableName;
    protected final ShardingInfo shardingInfoDigest;

    public AbstractMycatTable(String schemaName, String createTableSql, DrdsConst drdsConst) {
        this.schemaName = schemaName;
        this.createTableSql = createTableSql;
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSql);
        this.tableName = SQLUtils.normalize(sqlStatement.getTableName());
        this.relDataType = CalciteConvertors.getRelDataType(CalciteConvertors.getColumnInfo(createTableSql),
                MycatCalciteSupport.INSTANCE.TypeFactory
        );

        if (sqlStatement.isBroadCast()) {
            shardingInfoDigest = ShardingInfo.createBroadCast();
            return;
        }
        SQLMethodInvokeExpr dbPartitionBy = (SQLMethodInvokeExpr) sqlStatement.getDbPartitionBy();
        SQLExpr dbPartitions = sqlStatement.getDbPartitions();
        SQLMethodInvokeExpr tablePartitionBy = (SQLMethodInvokeExpr) sqlStatement.getTablePartitionBy();
        SQLExpr tablePartitions = sqlStatement.getTablePartitions();
        MySqlExtPartition exPartition = sqlStatement.getExtPartition();
        SQLName storedBy = sqlStatement.getStoredBy();
        SQLName distributeByType = sqlStatement.getDistributeByType();

        shardingInfoDigest = computeShardingInfo(
                dbPartitionBy,
                dbPartitions,
                tablePartitionBy,
                tablePartitions,
                exPartition,
                storedBy,
                distributeByType
        );

    }

    public abstract ShardingInfo computeShardingInfo(SQLMethodInvokeExpr dbPartitionBy, SQLExpr dbPartitions, SQLMethodInvokeExpr tablePartitionBy, SQLExpr tablePartitions, MySqlExtPartition exPartition, SQLName storedBy, SQLName distributeByType);

    public abstract Distribution computeDataNode(RexNode condition);

    public abstract Distribution computeDataNode();

    public ShardingInfo getShardingInfo() {
        return shardingInfoDigest;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return this.relDataType;
    }

    public RelDataType getRowType() {
        return getRowType(MycatCalciteSupport.INSTANCE.TypeFactory);
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    public boolean isBroadCast() {
        return this.shardingInfoDigest.getType() == ShardingInfo.Type.broadCast;
    }

    public boolean isNormal() {
        return this.shardingInfoDigest.getType() == ShardingInfo.Type.normal;
    }

    public boolean isSharding() {
        return this.shardingInfoDigest.getType() == ShardingInfo.Type.sharding;
    }

}