package io.mycat.mpp;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLLimit;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.SQLOrderBy;
import com.alibaba.fastsql.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLListExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.mpp.element.Order;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.plan.*;
import io.mycat.util.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqlToExprTranslator {
    final MyRelBuilder rexBuilder;
    final TranslatorBlackboard blackboard;

    public SqlToExprTranslator(MyRelBuilder rexBuilder, TranslatorBlackboard blackboard) {
        this.rexBuilder = rexBuilder;
        this.blackboard = blackboard;
    }


    public QueryPlan convertQueryRecursive(SQLObject sqlSelectQuery) {
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            return convertQuery((MySqlSelectQueryBlock) sqlSelectQuery);
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            return convertUnion((SQLUnionQuery) sqlSelectQuery);
        } else if (sqlSelectQuery instanceof SQLValuesQuery) {
            return convertValues((SQLValuesQuery) sqlSelectQuery);
        } else {
            throw new IllegalArgumentException();
        }
    }


    private QueryPlan convertUnion(SQLUnionQuery union) {
        List<SQLSelectQuery> relations = union.getRelations();
        for (SQLSelectQuery relation : relations) {
            SqlToExprTranslator sqlToExprTranslator = new SqlToExprTranslator(rexBuilder, blackboard);
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
        LogicTablePlan from = convertFrom(sqlSelectQuery.getFrom());

        Blackboard blackboard = new Blackboard(rexBuilder, from);
        SQLExpr where = sqlSelectQuery.getWhere();
        if (where != null) {
            rexBuilder.clear();
            rexBuilder.push(from);
            blackboard.addWhere(convertExpr(where));
        }
        List<SQLSelectItem> selectList = sqlSelectQuery.getSelectList();
        List<SQLAggregateExpr> aggregates = getAggregate(selectList);
        boolean isAggregate = sqlSelectQuery.getGroupBy() != null || !aggregates.isEmpty();
        rexBuilder.clear();
        rexBuilder.push(from);
        if (!isAggregate) {
            List<String> aliasList = new ArrayList<>(selectList.size());
            List<SqlValue> sqlValues = convertSelect(selectList, aliasList);
            blackboard.addSelect(sqlValues, aliasList);
        } else {
            AggregationStep agg = new AggBuilder(selectList, aggregates, sqlSelectQuery.getGroupBy()).build();
            blackboard.addGroupBy(agg);
        }
        SQLOrderBy orderBy = sqlSelectQuery.getOrderBy();
        if (orderBy != null) {
            blackboard.addOrderBy(convertOrderBy(orderBy));
        }
        SQLLimit limit = sqlSelectQuery.getLimit();
        if (limit != null) {
            convertLimit(blackboard, limit);
        }
        return blackboard.build();
    }


    private List<SQLAggregateExpr> getAggregate(List<SQLSelectItem> selectList) {
        List<SQLAggregateExpr> sqlAggregateExprs = new ArrayList<>();
        int size = selectList.size();
        for (int i = 0; i < size; i++) {
            SQLSelectItem sqlSelectItem = selectList.get(i);
            int o = sqlAggregateExprs.size();
            sqlSelectItem.accept(new MySqlASTVisitorAdapter() {
                @Override
                public void endVisit(SQLAggregateExpr x) {
                    sqlAggregateExprs.add(x);
                    super.endVisit(x);
                }
            });
            boolean notChange = sqlAggregateExprs.size() == o;
            if (notChange) {
                sqlAggregateExprs.add(null);
            }
        }
        return sqlAggregateExprs;
    }

    private boolean isAggregate(List<SqlValue> sqlValues) {
        return false;
    }

    private void convertLimit(Blackboard blackboard, SQLLimit limit) {
        long offset = Optional.ofNullable(limit.getOffset()).map(i -> (SQLNumericLiteralExpr) i).map(i -> i.getNumber().longValue()).orElse(0L);
        long to = Optional.of(limit.getRowCount()).map(i -> (SQLNumericLiteralExpr) i).map(i -> i.getNumber().longValue()).get();
        blackboard.addLimit(offset, to);
    }


    private List<Order> convertOrderBy(SQLOrderBy orderBy) {
        ArrayList<Order> list = new ArrayList<>();
        for (SQLSelectOrderByItem item : orderBy.getItems()) {
            SqlValue sqlAbs = convertExpr(item.getExpr());
            list.add(new Order(sqlAbs, item.getType()));
        }
        return list;
    }

    private List<SqlValue> convertSelect(List<SQLSelectItem> selectList, List<String> fieldLists) {
        ArrayList<SqlValue> absArrayList = new ArrayList<>();
        for (SQLSelectItem sqlSelectItem : selectList) {
            SqlValue expr = convertExpr(sqlSelectItem.getExpr());
            if (sqlSelectItem.getAlias() != null) {
                String alias = sqlSelectItem.computeAlias();
                if (alias != null) {
                    fieldLists.add(alias);
                    absArrayList.add(expr);
                    continue;
                }
            }
            fieldLists.add(sqlSelectItem.toString());
            absArrayList.add(expr);

        }
        return absArrayList;
    }

    private SqlValue convertExpr(SQLExpr expr) {
        RexTranslator rexTranslator = new RexTranslator(rexBuilder, blackboard);
        expr.accept(rexTranslator);
        return Objects.requireNonNull(rexTranslator.result);
    }


    private LogicTablePlan convertFrom(SQLTableSource from) {
        LogicTablePlan plan = null;
        try {
            if (from == null) {
                return new DualPlan();
            }
            if (from instanceof SQLExprTableSource) {
                plan = Objects.requireNonNull(convertExprTable((SQLExprTableSource) from));
            } else if (from instanceof SQLJoinTableSource) {
                plan = Objects.requireNonNull(convertJoinTable((SQLJoinTableSource) from));
            } else if (from instanceof SQLUnionQueryTableSource) {
                plan = Objects.requireNonNull(convertUnionTable((SQLUnionQueryTableSource) from));
            } else if (from instanceof SQLSubqueryTableSource) {
                plan = Objects.requireNonNull(convertSubqueryTable((SQLSubqueryTableSource) from));
            } else if (from instanceof SQLValuesTableSource) {
                plan = Objects.requireNonNull(convertValuesTable((SQLValuesTableSource) from));
            }
            return plan;
        } finally {
            this.blackboard.addTable(from.computeAlias(), plan);
        }
    }

    private LogicTablePlan convertValuesTable(SQLValuesTableSource from) {
        for (SQLListExpr value : from.getValues()) {
            List<SQLExpr> items = value.getItems();
            for (SQLExpr item : items) {
                SqlValue sqlAbs = convertExpr(item);
            }
        }
        throw new UnsupportedOperationException();//类型问题,暂时不实现

    }

    private LogicTablePlan convertSubqueryTable(SQLSubqueryTableSource from) {
        SQLSelectQuery query = from.getSelect().getQuery();
        QueryPlan sqlAbs = convertQueryRecursive(query);
        List<String> strings = from.getSelect().computeSelecteListAlias();
        rexBuilder.push(sqlAbs);
        rexBuilder.rename(strings);
        if (from.getAlias() != null) {
            rexBuilder.alias(from.getAlias2());
        }
        return (LogicTablePlan) rexBuilder.build();
    }

    private LogicTablePlan convertUnionTable(SQLUnionQueryTableSource from) {
        SQLUnionQuery union = from.getUnion();
        rexBuilder.push(convertUnion(union));
        if (from.getAlias() != null) {
            rexBuilder.alias(from.getAlias2());
        }
        return (LogicTablePlan) rexBuilder.build();
    }

    private LogicTablePlan convertJoinTable(SQLJoinTableSource from) {
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
        return (LogicTablePlan) rexBuilder.build();
    }

    private LogicTablePlan convertExprTable(SQLExprTableSource from) {
        if (from.getSchema() == null)
            from.setSchema(rexBuilder.getDefaultSchema());//from.getExpr()类型必为SQLPropertyExpr
        String schemaName = SQLUtils.normalize(from.getSchema());
        String tableName = SQLUtils.normalize(from.getTableName());
        if (from.getAlias() != null) {
            rexBuilder.alias(from.getAlias2());
        }
        return (LogicTablePlan) rexBuilder.scan(schemaName, tableName).build();
    }

    private class AggBuilder {
        private List<SQLSelectItem> selectList;
        private List<SQLAggregateExpr> aggregates;
        private SQLSelectGroupByClause groupBy;
        private List<SqlValue> inputRow;
        private List<SqlValue> outputRow;
        private Map<Integer, Pair<Integer, Integer>> indexMap = new HashMap<>();
        private List<AggSqlValue> agg = new ArrayList<>();
        private List<AccessDataExpr> groupByItems = new ArrayList<>();

        public AggBuilder(List<SQLSelectItem> selectList, List<SQLAggregateExpr> aggregates, SQLSelectGroupByClause groupBy) {
            this.selectList = selectList;
            this.aggregates = aggregates;
            this.groupBy = groupBy;
        }

        public AggregationStep build() {
            inputRow = getInputAggregateExpr(aggregates, selectList, indexMap);
            outputRow = getOutputAggregateExpr(aggregates, selectList, inputRow, indexMap);
            SqlValue havingExpr = null;

            MockQueryPlan mockQueryPlan = MockQueryPlan.create(RowType.of(outputRow));
            if (groupBy.getItems() != null) {
                rexBuilder.clear().push(mockQueryPlan);
                for (SQLExpr item : groupBy.getItems()) {
                    groupByItems.add((AccessDataExpr) convertExpr(item));
                }
            }
            if (groupBy.getHaving() != null) {
                SQLExpr having = groupBy.getHaving();
                havingExpr = getHavingExpr(having);
            }
            List<String> aliasList = new ArrayList<>(selectList.size());
//            outputRow= convertSelect(selectList, aliasList);

            return AggregationStepImpl.create(selectList, inputRow, outputRow, agg, groupByItems,havingExpr,null ,groupBy);
        }

        private SqlValue getHavingExpr(SQLExpr having) {
            rexBuilder.clear().push(MockQueryPlan.create(RowType.of(outputRow)));
            return convertExpr(having);
        }

        private List<SqlValue> getOutputAggregateExpr(List<SQLAggregateExpr> aggregates,
                                                      List<SQLSelectItem> selectList,
                                                      List<SqlValue> inputAggregateExpr,
                                                      Map<Integer, Pair<Integer, Integer>> indexMap) {
            int size = aggregates.size();
            int index = 0;

            List<SqlValue> out = new ArrayList<>(size);
            out.addAll(inputAggregateExpr);
            for (int i = 0; i < size; i++) {
                SQLAggregateExpr e = aggregates.get(i);
                //该列没有聚合函数
                if (e == null) {
                    SqlValue sqlValue = inputAggregateExpr.get(i);
                    out.add(AccessDataExpr.of(i, sqlValue.getType(), sqlValue.toString()));
                    continue;
                }
                //聚合函数的有参数
                SQLAggregateExpr sqlAggregateExpr = aggregates.get(i);
                List<SQLExpr> arguments = sqlAggregateExpr.getArguments();
                AggSqlValue sqlValue;
                List<SqlValue> args = Collections.emptyList();
                if (!arguments.isEmpty()) {
                    Pair<Integer, Integer> pair = indexMap.get(i);
                    args = IntStream.range(pair.getKey(), pair.getValue()).mapToObj(j -> {
                        SqlValue sql = inputAggregateExpr.get(j);
                        return AccessDataExpr.of(j, sql.getType(), sql.toParseTree().toString());
                    }).collect(Collectors.toList());
                }
                sqlValue = (AggSqlValue) rexBuilder.aggCall(sqlAggregateExpr.getMethodName(), args, false);
                agg.add(sqlValue);
                AccessDataExpr of = AccessDataExpr.of(i, sqlValue.getType(), sqlAggregateExpr.toString());

                if (sqlAggregateExpr.getParent() instanceof SQLSelectItem) {
                    out.add(of);
                    continue;
                }

                //叶子节点的聚合函数使用visitor替代结果
                RexTranslator rexTranslator = new RexTranslator(rexBuilder, blackboard) {
                    @Override
                    protected SqlValue convertExpr(SQLExpr expr) {
                        if (expr instanceof SQLAggregateExpr) {
                            return collect(expr, of);
                        } else {
                            return super.convertExpr(expr);
                        }
                    }

                    @Override
                    public void endVisit(SQLAggregateExpr node) {
                        collect(node, of);
                    }
                };
                selectList.get(i).accept(rexTranslator);
                out.add(rexTranslator.result);
            }
            return out;
        }


        private List<SqlValue> getInputAggregateExpr(List<SQLAggregateExpr> aggregates,
                                                     List<SQLSelectItem> selectList,
                                                     Map<Integer, Pair<Integer, Integer>> indexMap) {
            int size = aggregates.size();
            List<SQLExpr> in = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                SQLAggregateExpr e = aggregates.get(i);
                //该列没有聚合函数
                if (e == null) {
                    in.add(selectList.get(i).getExpr());
                    continue;
                }
                //聚合函数的有参数
                List<SQLExpr> arguments = aggregates.get(i).getArguments();
                int startIndex = in.size();
                in.addAll(arguments);
                int endIndex = in.size();
                indexMap.put(i, Pair.of(startIndex, endIndex));
            }
            return in.stream().distinct().map(i -> convertExpr(i)).collect(Collectors.toList());
        }
    }
}