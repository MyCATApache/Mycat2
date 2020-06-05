/**
 * Copyright (C) <2019>  <chen junwen>
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
import io.mycat.SchemaInfo;
import io.mycat.calcite.table.MycatPhysicalTable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Junwen Chen
 **/
public class MycatImplementor extends RelToSqlConverter {
    @Override
    public Result visit(TableScan e) {
        try {
            MycatPhysicalTable physicalTable = e.getTable().unwrap(MycatPhysicalTable.class);
            if (physicalTable != null) {
                SchemaInfo schemaInfo = physicalTable.getBackendTableInfo().getSchemaInfo();
                SqlIdentifier identifier;
                if (schemaInfo.getTargetSchema() == null) {
                    identifier = new SqlIdentifier(Collections.singletonList(schemaInfo.getTargetTable()), SqlParserPos.ZERO);
                } else {
                    identifier = new SqlIdentifier(Arrays.asList(schemaInfo.getTargetSchema(), schemaInfo.getTargetTable()), SqlParserPos.ZERO);
                }
                return result(identifier, ImmutableList.of(Clause.FROM), e, null);
            } else {
                return super.visit(e);
            }
        } catch (Throwable e1) {
            return null;
        }

    }
//    public static String toString(RelNode node) {
//        try {
//            MycatImplementor dataNodeSqlConverter = new MycatImplementor();
//            SqlImplementor.Result visit = dataNodeSqlConverter.visitChild(0, node);
//            SqlNode sqlNode = visit.asStatement();
//            return sqlNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    public MycatImplementor(SqlDialect dialect) {
        super(dialect);
    }

//    /** @see #dispatch */
//    public Result visit(MycatTransientSQLTableScan scan) {
//        return scan.implement();

//    }

    public Result implement(RelNode node) {
        return dispatch(node);
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
            builder.setSelect(new SqlNodeList(Collections.singleton(SqlLiteral.createNull(POS)), POS));
            return builder.result();
        }
        return super.visit(e);
    }

    @Override
    public Result visit(Union e) {
        if (!e.isDistinct()) {
            ArrayList<RelNode> unions = new ArrayList<>();
            CalciteUtls.collect(e, unions);
            RelBuilder relBuilder = MycatCalciteSupport.INSTANCE.relBuilderFactory.create(e.getCluster(), null);
            relBuilder.pushAll(unions);
            relBuilder.union(e.all, unions.size());
            e = (Union) relBuilder.build();
            SqlNode node = null;
            for (Ord<RelNode> input : Ord.zip(e.getInputs())) {
                final Result result = visitChild(input.i, input.e);
                if (node == null) {
                    node = result.subSelect();//修改点 会添加别名???
                } else {
                    SqlSetOperator sqlSetOperator = e.all
                            ? SqlStdOperatorTable.UNION_ALL
                            : SqlStdOperatorTable.UNION;
                    node = sqlSetOperator.createCall(POS, node, result.asSelect());
                }
            }
            final List<Clause> clauses =
                    Expressions.list(Clause.SET_OP);
            return result(node, clauses, e, null);
        }
        return super.visit(e);
    }



    @Override
    protected Result buildAggregate(Aggregate e, Builder builder,
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
        return builder.result();
    }
}