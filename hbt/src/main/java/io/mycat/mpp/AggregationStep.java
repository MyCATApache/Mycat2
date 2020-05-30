package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;

import java.util.List;

public interface AggregationStep {

    List<SQLSelectItem> getSQLSelectItemList();

    List<SqlValue> getInput();

    List<SqlValue> getOutput();
    List<SqlValue> getProject();

    List<AggSqlValue> getAggregationExpr();

    SqlValue getHavingExpr();

    SQLSelectGroupByClause getSQLSelectGroupByClause();

    List<AccessDataExpr> getGroupByItems();

}