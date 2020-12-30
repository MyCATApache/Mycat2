package io.mycat.mpp;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import io.mycat.mpp.element.Order;
import io.mycat.mpp.plan.DualPlan;
import io.mycat.mpp.plan.QueryPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlToExprTranslator {
    final MyRelBuilder rexBuilder;

    public SqlToExprTranslator(MyRelBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }


   public QueryPlan convertQueryRecursive(SQLObject sqlSelectQuery) {
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            return convertQuery((MySqlSelectQueryBlock) sqlSelectQuery);
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            return convertUnion((SQLUnionQuery) sqlSelectQuery);
        } else if (sqlSelectQuery instanceof SQLValuesQuery) {
            return convertValues((SQLValuesQuery) sqlSelectQuery);
        }else {
            throw new IllegalArgumentException();
        }
    }


    private QueryPlan convertUnion(SQLUnionQuery union) {
        List<SQLSelectQuery> relations = union.getRelations();
        for (SQLSelectQuery relation : relations) {
            SqlToExprTranslator sqlToExprTranslator = new SqlToExprTranslator(rexBuilder);
            QueryPlan sqlAbs = sqlToExprTranslator.convertQueryRecursive(relation);
            rexBuilder.push(sqlAbs);
        }
        switch (union.getOperator()) {
            case UNION:
            case UNION_ALL:
                rexBuilder.union(true, relations.size());
                break;
            case MINUS:
                rexBuilder.union(false, relations.size());
                break;
            case EXCEPT:
                rexBuilder.except(false, relations.size());
                break;
            case INTERSECT:
                rexBuilder.intersect(false, relations.size());
                break;
            case DISTINCT:
                rexBuilder.distinct(relations.size());
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + union.getOperator());
        }
        Blackboard blackboard = new Blackboard(rexBuilder, rexBuilder.build());

        SQLOrderBy orderBy = union.getOrderBy();
        if (orderBy != null) {
            blackboard.addOrderBy(convertOrderBy(orderBy));
        }
        SQLLimit limit = union.getLimit();
        if (limit != null) {
            convertLimit(blackboard, limit);
        }
        return blackboard.build();
    }

    private QueryPlan convertValues(SQLValuesQuery sqlSelectQuery) {
        List<SQLExpr> values = sqlSelectQuery.getValues();
        return null;
    }

    private QueryPlan convertQuery(MySqlSelectQueryBlock sqlSelectQuery) {
        Blackboard blackboard = new Blackboard(rexBuilder, convertFrom(sqlSelectQuery.getFrom()));
        SQLExpr where = sqlSelectQuery.getWhere();
        if (where != null) {
            blackboard.addWhere(convertExpr(where));
        }
        List<SQLSelectItem> selectList = sqlSelectQuery.getSelectList();
        if (selectList != null) {
            blackboard.addSelect(convertSelect(selectList));
        }
        SQLOrderBy orderBy = sqlSelectQuery.getOrderBy();
        if (orderBy != null) {
            blackboard.addOrderBy(convertOrderBy(orderBy));
        }
        SQLSelectGroupByClause groupBy = sqlSelectQuery.getGroupBy();
        if (groupBy != null) {
            convertGroupBy(blackboard, groupBy);
        }
        SQLLimit limit = sqlSelectQuery.getLimit();
        if (limit != null) {
            convertLimit(blackboard, limit);
        }
        return blackboard.build();
    }

    private void convertLimit(Blackboard blackboard, SQLLimit limit) {
        long offset = Optional.ofNullable(limit.getOffset()).map(i -> (SQLNumericLiteralExpr) i).map(i -> i.getNumber().longValue()).orElse(0L);
        long to = Optional.of(limit.getRowCount()).map(i -> (SQLNumericLiteralExpr) i).map(i -> i.getNumber().longValue()).get();
        blackboard.addLimit(offset, to);
    }

    private void convertGroupBy(Blackboard blackboard, SQLSelectGroupByClause groupBy) {
        List<SqlValue> groupList = new ArrayList<>();
        for (SQLExpr item : groupBy.getItems()) {
            groupList.add(convertExpr(item));
        }
        SQLExpr having = groupBy.getHaving();
        if (having != null) {
            blackboard.addGroupBy(groupList, convertExpr(having));
        } else {
            blackboard.addGroupBy(groupList, null);
        }
    }

    private List<Order> convertOrderBy(SQLOrderBy orderBy) {
        ArrayList<Order> list = new ArrayList<>();
        for (SQLSelectOrderByItem item : orderBy.getItems()) {
            SqlValue sqlAbs = convertExpr(item.getExpr());
            list.add(new Order(sqlAbs, item.getType()));
        }
        return list;
    }

    private List<SqlValue> convertSelect(List<SQLSelectItem> selectList) {
        ArrayList<SqlValue> absArrayList = new ArrayList<>();
        for (SQLSelectItem sqlSelectItem : selectList) {
            SqlValue expr = convertExpr(sqlSelectItem.getExpr());
            String alias = sqlSelectItem.computeAlias();
            if (alias != null) expr = (rexBuilder.alias(expr, alias));
            absArrayList.add(expr);
        }
        return absArrayList;
    }

    private SqlValue convertExpr(SQLExpr where) {
        RexTranslator rexTranslator = new RexTranslator(rexBuilder);
        where.accept(rexTranslator);
        return rexTranslator.result;
    }


    private QueryPlan convertFrom(SQLTableSource from) {
        if (from == null){
            return new DualPlan();
        }
        if (from instanceof SQLExprTableSource) {
            return convertExprTable((SQLExprTableSource) from);
        } else if (from instanceof SQLJoinTableSource) {
            return convertJoinTable((SQLJoinTableSource) from);
        } else if (from instanceof SQLUnionQueryTableSource) {
            return convertUnionTable((SQLUnionQueryTableSource) from);
        } else if (from instanceof SQLSubqueryTableSource) {
            return convertSubqueryTable((SQLSubqueryTableSource) from);
        } else if (from instanceof SQLValuesTableSource) {
            return convertValuesTable((SQLValuesTableSource) from);
        }
        throw new IllegalArgumentException();
    }

    private QueryPlan convertValuesTable(SQLValuesTableSource from) {
        for (SQLListExpr value : from.getValues()) {
            List<SQLExpr> items = value.getItems();
            for (SQLExpr item : items) {
                SqlValue sqlAbs = convertExpr(item);
            }
        }
        throw new UnsupportedOperationException();//类型问题,暂时不实现

    }

    private QueryPlan convertSubqueryTable(SQLSubqueryTableSource from) {
        SQLSelectQuery query = from.getSelect().getQuery();
        QueryPlan sqlAbs = convertQueryRecursive(query);
        List<String> strings = from.getSelect().computeSelecteListAlias();
        rexBuilder.push(sqlAbs);
        rexBuilder.rename(strings);
        if (from.getAlias() != null) {
            rexBuilder.alias(from.getAlias2());
        }
        return rexBuilder.build();
    }

    private QueryPlan convertUnionTable(SQLUnionQueryTableSource from) {
        SQLUnionQuery union = from.getUnion();
        rexBuilder.push(convertUnion(union));
        if (from.getAlias() != null) {
            rexBuilder.alias(from.getAlias2());
        }
        return rexBuilder.build();
    }

    private QueryPlan convertJoinTable(SQLJoinTableSource from) {
        QueryPlan left = convertFrom(from.getLeft());
        SQLJoinTableSource.JoinType joinType = from.getJoinType();
        QueryPlan right = convertFrom(from.getRight());
        SqlValue condition = convertExpr(from.getCondition());
        final List<SqlValue> using =
                Optional.ofNullable(from.getUsing()).filter(i -> !i.isEmpty())
                .map(i -> i.stream().map(j -> convertExpr(j)).collect(Collectors.toList())).orElse(null);
        boolean natural = from.isNatural();
     rexBuilder.push(left).push(right).join(joinType, condition, using, natural);
        if (from.getAlias() != null) {
            rexBuilder.alias(from.getAlias2());
        }
        return rexBuilder.build();
    }

    private QueryPlan convertExprTable(SQLExprTableSource from) {
        if (from.getSchema() == null)
            from.setSchema(rexBuilder.getDefaultSchema());//from.getExpr()类型必为SQLPropertyExpr
        String schemaName = SQLUtils.normalize(from.getSchema());
        String tableName = SQLUtils.normalize(from.getTableName());
        return rexBuilder.scan(schemaName,tableName).build();
    }
}