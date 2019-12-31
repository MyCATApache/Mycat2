package io.mycat.sqlEngine.ast.optimizer;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.*;
/**
 * @author Junwen Chen
 **/
public class SubqueryOptimizer extends MySqlASTVisitorAdapter {

    final List<SQLExpr> currentColumnList = new ArrayList<>();
    final LinkedList<MySqlSelectQueryBlock> stack = new LinkedList<>();
    Map<MySqlSelectQueryBlock, CorrelatedQuery> correlateQueries;
    List<MySqlSelectQueryBlock> normalQueries;


    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        stack.push(x);
        return super.visit(x);
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {
        boolean correlated = false;
        for (SQLExpr sqlExpr : currentColumnList) {
            correlated = correlated || findOuterRef(sqlExpr);
        }
        MySqlSelectQueryBlock current = stack.pop();
        if (!correlated && !stack.isEmpty()) {
            addNormalQuery(current);
        }
        currentColumnList.clear();
        super.endVisit(x);
    }


    @Override
    public void endVisit(SQLIdentifierExpr x) {
        currentColumnList.add(x);
        super.endVisit(x);
    }

    @Override
    public void endVisit(SQLPropertyExpr x) {
        currentColumnList.add(x);
        super.endVisit(x);
    }

    private boolean findOuterRef(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLIdentifierExpr) {
            return findOuterRef(sqlExpr, ((SQLIdentifierExpr) sqlExpr).getResolvedColumn(),
                    ((SQLIdentifierExpr) sqlExpr).getResolvedTableSource());
        } else {
            return findOuterRef(sqlExpr, ((SQLPropertyExpr) sqlExpr).getResolvedColumn(),
                    ((SQLPropertyExpr) sqlExpr).getResolvedTableSource());
        }
    }

    private boolean findOuterRef(SQLExpr x,
                                 SQLColumnDefinition resolvedColumn,
                                 SQLTableSource resolvedTableSource) {
        if (resolvedTableSource != null && resolvedColumn != null && !stack.isEmpty()) {
            MySqlSelectQueryBlock currentQuery = stack.peek();
            SQLTableSource currentDatasource = currentQuery.getFrom();
            if (!resolvedTableSource.equals(currentDatasource)) {
                addCorrelatedQuery(currentQuery, x, resolvedTableSource);
                return true;
            } else {
                addInColumn(currentQuery, x, resolvedTableSource);
            }
        }
        return false;
    }

    private void addInColumn(MySqlSelectQueryBlock currentQuery, SQLExpr x, SQLTableSource resolvedTableSource) {
        checkInitCorrelateQueriesCollector();
        CorrelatedQuery correlatedQuery = correlateQueries.get(currentQuery);
        if (correlatedQuery == null) {
            correlatedQuery = new CorrelatedQuery(currentQuery);
        }
        correlatedQuery.addInColumnRef(resolvedTableSource, x);
    }

    private void addNormalQuery(MySqlSelectQueryBlock current) {
        checkInitNormalQueriesCollector();
        normalQueries.add(current);
    }


    private void addCorrelatedQuery(MySqlSelectQueryBlock currentQuery, SQLExpr x, SQLTableSource resolvedTableSource) {
        checkInitCorrelateQueriesCollector();
        CorrelatedQuery correlatedQuery = correlateQueries.get(currentQuery);
        if (correlatedQuery == null) {
            correlatedQuery = new CorrelatedQuery(currentQuery);
        }
        correlatedQuery.addOutColumnRef(resolvedTableSource, x);
        correlateQueries.put(currentQuery, correlatedQuery);
    }

    private void checkInitCorrelateQueriesCollector() {
        if (correlateQueries == null) {
            correlateQueries = new IdentityHashMap<>();
        }
    }

    private void checkInitNormalQueriesCollector() {
        if (normalQueries == null) {
            normalQueries = new ArrayList<>();
        }
    }

    public List<MySqlSelectQueryBlock> getNormalQueries() {
        return normalQueries == null ? Collections.emptyList() : normalQueries;
    }

    public Map<MySqlSelectQueryBlock,CorrelatedQuery> getCorrelateQueries() {
        return correlateQueries;
    }

    public static class CorrelatedQuery {
        final MySqlSelectQueryBlock queryBlock;
        final Map<SQLExpr, SQLTableSource> outColumn = new HashMap<>();
        final Map<SQLExpr, SQLTableSource> inColumn = new HashMap<>();

        public CorrelatedQuery(MySqlSelectQueryBlock queryBlock) {
            this.queryBlock = queryBlock;
        }

        void addOutColumnRef(SQLTableSource tableSource, SQLExpr column) {
            outColumn.put(column, tableSource);
        }

        public Map<SQLExpr, SQLTableSource> getOutColumn() {
            return outColumn;
        }

        void addInColumnRef(SQLTableSource tableSource, SQLExpr column) {
            inColumn.put(column, tableSource);
        }

        public Map<SQLExpr, SQLTableSource> getInColumn() {
            return inColumn;
        }
    }

}