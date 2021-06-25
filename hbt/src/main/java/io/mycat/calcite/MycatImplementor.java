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
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.TableHandler;
import io.mycat.calcite.physical.MycatTableLookupValues;
import io.mycat.calcite.table.MycatLogicTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Junwen Chen
 **/
public class MycatImplementor extends RelToSqlConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatImplementor.class);

    public MycatImplementor(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public Context aliasContext(Map<String, RelDataType> aliases, boolean qualified) {
        return super.aliasContext(aliases, qualified);
    }

    @Override
    public Result visit(TableScan e) {
        try {

            MycatLogicTable logicTable = e.getTable().unwrap(MycatLogicTable.class);
            if (logicTable != null) {
                String hintText = "";
                if (dialect instanceof MysqlSqlDialect) {
                    for (RelHint hint : e.getHints()) {
                        if ("INDEX".equalsIgnoreCase(hint.hintName)) {
                            hintText = "USE INDEX(" + String.join(",", hint.listOptions) + ")";
                        }
                    }
                }
                TableHandler tableHandler = logicTable.logicTable();
                SqlNode tableParamSqlNode = new TableParamSqlNode(ImmutableList.of(tableHandler.getSchemaName(),tableHandler.getTableName()),tableHandler.getUniqueName(), hintText);

                return result(tableParamSqlNode, ImmutableList.of(Clause.FROM), e, null);
            }
            return super.visit(e);
        } catch (Throwable e1) {
            LOGGER.error("", e1);
            return null;
        }
    }

    public static final SqlValuesOperator MYCAT_SQL_VAULES = new SqlValuesOperator() {
        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            writer.print("VALUES");
            final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.VALUES, " ", " ");
            for (SqlNode operand : call.getOperandList()) {
                writer.sep(",");
                writer.print(operand.toString());
            }
            writer.endList(frame);
        }
    };
    public static final SqlBinaryOperator MYCAT_SQL_LOOKUP_IN = new SqlBinaryOperator("IN", SqlKind.OTHER, 32, true,
            ReturnTypes.BOOLEAN_NULLABLE,
            InferTypes.FIRST_KNOWN,
            null) {
    };

//    @Override
//    public Context aliasContext(Map<String, RelDataType> aliases, boolean qualified) {
//        return new AliasContext(dialect, aliases, qualified);
//    }
public static final SqlSpecialOperator ROW = new MycatSqlRowOperator("MYCAT_ROW");
    public Result visit(MycatTableLookupValues e) {
        RelDataType rowType = e.getRowType();
        int fieldCount = rowType.getFieldCount();
        ImmutableList.Builder<SqlNode> builder = ImmutableList.builder();
        Context context = aliasContext(Collections.emptyMap(),false);
        for (RexNode expr : e.getExprs()) {
            SqlNode sqlNode = context.toSql(null, expr);
            builder.add(sqlNode);
        }
        SqlBasicCall sqlBasicCall = new SqlBasicCall(MYCAT_SQL_VAULES, builder.build().toArray(new SqlNode[]{}), SqlParserPos.ZERO);
        return result(sqlBasicCall, ImmutableList.of(Clause.FROM), e, null);
    }


//    public MycatImplementor(SqlDialect dialect) {
//        this(dialect);
//    }


    public Result implement(RelNode node) {
        return visitRoot(node);
    }

    /**
     * 1.修复生成的sql不带select items
     *
     * @param e
     * @return
     */
    @Override
    public Result visit(Project e) {
        if (e.getProjects().isEmpty()) {
            Result x = visitChild(0, e.getInput());
            final Builder builder =
                    x.builder(e, Clause.SELECT);
            builder.setSelect(new SqlNodeList(Collections.singleton(SqlLiteral.createApproxNumeric("1", POS)), POS));
            return builder.result();
        }
        return super.visit(e);
    }

    @Override
    protected Builder buildAggregate(Aggregate e, Builder builder,
                                     List<SqlNode> selectList, List<SqlNode> groupByList) {
        for (AggregateCall aggCall : e.getAggCallList()) {

            RelDataType type = aggCall.type;
            SqlNode aggCallSqlNode = builder.context.toSql(aggCall);
            if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
                aggCallSqlNode = dialect.rewriteSingleValueExpr(aggCallSqlNode);
                aggCallSqlNode = SqlStdOperatorTable.CAST.createCall(POS,
                        aggCallSqlNode, dialect.getCastSpec(type));
            }
            addSelect(selectList, aggCallSqlNode, e.getRowType());
        }
        builder.setSelect(new SqlNodeList(selectList, POS));
        if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
            // Some databases don't support "GROUP BY ()". We can omit it as long
            // as there is at least one aggregate function.
            builder.setGroupBy(new SqlNodeList(groupByList, POS));
        }
        return builder;
    }

    private Sort computeSortFetch(Sort e, Number first, Number second) {
        RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
        e = e.copy(e.getTraitSet(), e.getInput(), e.getCollation(), e.offset, rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(first.longValue() + second.longValue())));
        return e;
    }

//    @Override
//    public Result visit(Sort e) {
//        RexNode fetch = e.fetch;
//        if (fetch != null && fetch.getKind() == SqlKind.PLUS) {
//
//            RexCall fetch1 = (RexCall) fetch;
//            if (params!=null&&!params.isEmpty()) {
//                List<RexNode> operands = fetch1.getOperands();
//                RexNode offsetRexNode = operands.get(0);
//                RexNode limitRexNode = operands.get(1);
//                if (offsetRexNode instanceof RexDynamicParam && limitRexNode instanceof RexDynamicParam) {
//                    RexDynamicParam left = (RexDynamicParam) operands.get(0);
//                    RexDynamicParam right = (RexDynamicParam) operands.get(1);
//                    Number first = (Number) params.get(left.getIndex());
//                    Number second = (Number) params.get(right.getIndex());
//                    e = computeSortFetch(e, first, second);
//                } else if (offsetRexNode instanceof RexLiteral && limitRexNode instanceof RexLiteral) {
//                    e = computeSortFetch(e, ((RexLiteral) offsetRexNode).getValueAs(Long.class), ((RexLiteral) limitRexNode).getValueAs(Long.class));
//                }
//            } else {
//                List<RexNode> operands = fetch1.getOperands();
//                RexLiteral offsetRexNode = (RexLiteral) operands.get(0);
//                RexLiteral limitRexNode = (RexLiteral) operands.get(1);
//                e = computeSortFetch(e, ((Number) offsetRexNode.getValue()).longValue(), ((Number) limitRexNode.getValue()).longValue());
//            }
//
//        }
//        return super.visit(e);
//    }
}