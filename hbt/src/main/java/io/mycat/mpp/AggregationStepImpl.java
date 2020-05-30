package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;

import java.util.List;
import java.util.Objects;


public class AggregationStepImpl implements AggregationStep {
    final List<SQLSelectItem> sqlSelectItems;
    final List<SqlValue> inputs;
    final List<SqlValue> outputs;
    final List<AggSqlValue> aggregationExpr;
    final List<AccessDataExpr> groupByItems;
    final SqlValue havingExpr;
    final SQLSelectGroupByClause sQLSelectGroupByClause;
    final List<SqlValue> project;

    public static AggregationStep create(List<SQLSelectItem> sqlSelectItems,
                                         List<SqlValue> inputs,
                                         List<SqlValue> outputs,
                                         List<AggSqlValue> aggregationExpr,
                                         List<AccessDataExpr> groupByItems,
                                         SqlValue havingExpr,
                                         List<SqlValue> project,
                                         SQLSelectGroupByClause sQLSelectGroupByClause) {
        return new AggregationStepImpl(sqlSelectItems, inputs, outputs, aggregationExpr, groupByItems, havingExpr, project, sQLSelectGroupByClause);
    }

    public AggregationStepImpl(List<SQLSelectItem> sqlSelectItems, List<SqlValue> inputs, List<SqlValue> outputs, List<AggSqlValue> aggregationExpr
            , List<AccessDataExpr> groupByItems, SqlValue havingExpr, List<SqlValue> project, SQLSelectGroupByClause sQLSelectGroupByClause) {
        this.sqlSelectItems = Objects.requireNonNull(sqlSelectItems);
        this.inputs = Objects.requireNonNull(inputs);
        this.outputs = Objects.requireNonNull(outputs);
        this.aggregationExpr = Objects.requireNonNull(aggregationExpr);
        this.groupByItems = groupByItems;
        this.havingExpr = havingExpr;
        this.sQLSelectGroupByClause = Objects.requireNonNull(sQLSelectGroupByClause);
        this.project = project;
    }

    @Override
    public List<SQLSelectItem> getSQLSelectItemList() {
        return sqlSelectItems;
    }

    @Override
    public List<SqlValue> getInput() {
        return inputs;
    }

    @Override
    public List<SqlValue> getOutput() {
        return outputs;
    }

    @Override
    public List<SqlValue> getProject() {
        return project;
    }

    @Override
    public List<AggSqlValue> getAggregationExpr() {
        return aggregationExpr;
    }


    @Override
    public SqlValue getHavingExpr() {
        return havingExpr;
    }

    @Override
    public SQLSelectGroupByClause getSQLSelectGroupByClause() {
        return sQLSelectGroupByClause;
    }

    @Override
    public List<AccessDataExpr> getGroupByItems() {
        return groupByItems;
    }
}