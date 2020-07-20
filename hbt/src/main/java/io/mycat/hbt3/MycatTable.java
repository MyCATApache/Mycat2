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
import com.alibaba.fastsql.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExtPartition;
import com.google.common.collect.ImmutableList;
import io.mycat.calcite.CalciteConvertors;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.ShardingInfo;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Getter
public class MycatTable extends AbstractTable {
    final String createTableSql;
    final RelDataType relDataType;
    private String schemaName;
    private String tableName;
    private ShardingInfo shardingInfo;

    public MycatTable(String schemaName, String createTableSql, DrdsConst drdsConst) {
        this.schemaName = schemaName;
        this.createTableSql = createTableSql;
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSql);
        this.tableName = SQLUtils.normalize(sqlStatement.getTableName());
        this.relDataType = CalciteConvertors.getRelDataType(CalciteConvertors.getColumnInfo(createTableSql),
                MycatCalciteSupport.INSTANCE.TypeFactory
        );

        if (sqlStatement.isBroadCast()) {
            shardingInfo = new ShardingInfo(ShardingInfo.Type.broadCast, ImmutableList.of(), null, null, null,
                    drdsConst.getDatasourceNum(),
                    1, 1, null);
            return;
        }
        SQLMethodInvokeExpr dbPartitionBy = (SQLMethodInvokeExpr) sqlStatement.getDbPartitionBy();
        SQLExpr dbPartitions = sqlStatement.getDbPartitions();
        SQLMethodInvokeExpr tablePartitionBy = (SQLMethodInvokeExpr) sqlStatement.getTablePartitionBy();
        SQLExpr tablePartitions = sqlStatement.getTablePartitions();
        MySqlExtPartition exPartition = sqlStatement.getExtPartition();
        SQLName storedBy = sqlStatement.getStoredBy();
        SQLName distributeByType = sqlStatement.getDistributeByType();
        Function function;
        if (dbPartitionBy != null) {
            int dbPartitionNum = Optional.ofNullable(sqlStatement.getDbPartitions())
                    .map(i -> ((SQLNumericLiteralExpr) i).getNumber().intValue())
                    .orElse(drdsConst.getShardingSchemaNum());
            String methodName = dbPartitionBy.getMethodName().toUpperCase();
            ImmutableList.Builder<String> schemaColumnsbuilder = ImmutableList.builder();
            List<SQLExpr> arguments = dbPartitionBy.getArguments();
            if ("RANGE_HASH".equalsIgnoreCase(methodName)) {
                schemaColumnsbuilder.add(arguments.get(0).toString());
                schemaColumnsbuilder.add(arguments.get(1).toString());
                int startIndex = Integer.parseInt(arguments.get(2).toString());
            } else if ("RIGHT_SHIFT".equalsIgnoreCase(methodName)) {
                schemaColumnsbuilder.add(arguments.get(0).toString());
                int startIndex = Integer.parseInt(arguments.get(1).toString());
            } else if ("UNI_HASH".equalsIgnoreCase(methodName)) {
                schemaColumnsbuilder.add(arguments.get(0).toString());
            } else if ("STR_HASH".equalsIgnoreCase(methodName)) {
                schemaColumnsbuilder.add(arguments.get(0).toString());
                int startIndex = -1;
                int endIndex = -1;
                int varType = 0;
                int randSeed = 31;
                if (arguments.size() > 1) {
                    startIndex = Integer.parseInt(arguments.get(1).toString());
                    endIndex = Integer.parseInt(arguments.get(2).toString());
                }
                if (arguments.size() > 3) {
                    varType = Integer.parseInt(arguments.get(3).toString());
                }
                if (arguments.size() > 4) {
                    randSeed = Integer.parseInt(arguments.get(4).toString());
                }
                String value = null;
                if (value == null){
                    value = "";
                }
                if (startIndex>=0&&endIndex>=0){
                    value=   value.substring(startIndex,endIndex);
                }

            } else if ("HASH".equalsIgnoreCase(methodName)) {
                schemaColumnsbuilder.add(arguments.get(0).toString());
            } else {
                schemaColumnsbuilder.add(arguments.get(0).toString());
            }
            ImmutableList<String> schemaColumns = schemaColumnsbuilder.build();
            shardingInfo = new ShardingInfo(ShardingInfo.Type.sharding,
                    schemaColumns,
                    null,
                    methodName,
                    null,
                    drdsConst.getDatasourceNum(),
                    dbPartitionNum, 1, null);
        } else {
            //单表
            shardingInfo = new ShardingInfo(ShardingInfo.Type.normal, ImmutableList.of(), ImmutableList.of(),
                    null, null, drdsConst.getDatasourceNum(), 1, 1, null);
        }

    }

//    @NotNull
//    public Function<Object, Integer> createPartionFunction(SQLMethodInvokeExpr dbPartitionBy, int dbPartitionNum) {
//        String methodName = dbPartitionBy.getMethodName();
//        List<SQLExpr> arguments = Optional.ofNullable(dbPartitionBy.getArguments()).orElse(Collections.emptyList());
//        return PartitionMethodFactory
//                .getByName(methodName, arguments, relDataType, dbPartitionNum);
//    }

    public PartInfo computeDataNode(RexNode condition) {
        if (isBroadCast()) {
            return new SinglePartInfo(new PartImpl(shardingInfo.getDatasourceSize(), -1, -1));
        }
        if (condition.getKind() == SqlKind.EQUALS) {
            RexCall rexNode = (RexCall) condition;
            List<RexNode> operands = new ArrayList<>(rexNode.getOperands());
            RexNode rexNode1 = operands.get(0);
            RexNode rexNode2 = operands.get(1);

            String columnName = null;
            Object value = null;
            if (rexNode1 instanceof RexInputRef) {
                int index = ((RexInputRef) rexNode1).getIndex();
                List<String> fieldNames = getRowType().getFieldNames();
                columnName = fieldNames.get(index);
            }
            if (rexNode2 instanceof RexLiteral) {
                value = ((RexLiteral) rexNode2).getValue();
            }
            if (columnName != null) {
                String schemaFun = shardingInfo.getSchemaFun();
                String tableFun = shardingInfo.getTableFun();
                if (schemaFun != null) {
                    List<String> schemaKeys = shardingInfo.getSchemaKeys();
                    if (schemaKeys.contains(columnName)) {
                        switch (schemaFun) {
                            case "HASH": {
                                if (value instanceof Number) {
                                    int l = ((Number) value).intValue() % shardingInfo.getSchemaSize();
                                    return new SinglePartInfo(new PartImpl(shardingInfo.getDatasourceSize(), l, 0));
                                }
                                if (value == null) {
                                    value = "";
                                }
                                if (value instanceof String) {
                                    int l = value.hashCode() % shardingInfo.getSchemaSize();
                                    return new SinglePartInfo(new PartImpl(shardingInfo.getDatasourceSize(), l, 0));
                                }
                                throw new IllegalArgumentException("unsupported type:" + value.getClass());
                            }
                            case "STR_HASH": {

                            }
                        }
                    }

                }
            }
        }
        return computeDataNode();
    }

    public PartInfo computeDataNode() {
        return new RangePartInfo(0, shardingInfo.size(), shardingInfo);
    }

    public ShardingInfo getShardingInfo() {
        return shardingInfo;
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
        return this.shardingInfo.isBroadCast();
    }

    public boolean isNormal() {
        return this.shardingInfo.isNormal();
    }

    public boolean isSharding() {
        return this.shardingInfo.isSharding();
    }
}