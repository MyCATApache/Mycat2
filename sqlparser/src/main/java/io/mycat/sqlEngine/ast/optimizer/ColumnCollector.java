package io.mycat.sqlEngine.ast.optimizer;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.sqlEngine.ast.converter.Converters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;
/**
 * @author Junwen Chen
 **/
public class ColumnCollector extends MySqlASTVisitorAdapter {
    private boolean repairColumnNames;

    private final Map<SQLTableSource, Map<SQLExpr, SQLColumnDefinition>> tableSourceColumnMap = new IdentityHashMap<>();

    public ColumnCollector(boolean repairColumnNames) {
        this.repairColumnNames = repairColumnNames;
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        SQLTableSource from = x.getFrom();
        if (from != null) {
            tableSourceColumnMap.put(from, new HashMap<>());
        }
        return super.visit(x);
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {
        SQLTableSource from = x.getFrom();
        if (from != null) {
            Map<SQLExpr, SQLColumnDefinition> selectColumn = tableSourceColumnMap.get(from);
            if (selectColumn != null) {
                HashSet<SQLColumnDefinition> selectSet = new HashSet<>();
                for (SQLSelectItem sqlSelectItem : x.getSelectList()) {
                    SQLExpr expr = sqlSelectItem.getExpr();
                    if (expr instanceof SQLName) {
                        SQLColumnDefinition columnDef = Converters.getColumnDef((SQLName) expr);
                        if (columnDef != null) {
                            selectSet.add(columnDef);
                        }
                    }
                }
                //可选,对缺少的列补全,但是不应该作为最终结果集输出
                if (repairColumnNames) {
                    for (Entry<SQLExpr, SQLColumnDefinition> entry : selectColumn
                            .entrySet()) {
                        if (!selectSet.contains(entry.getValue())) {
                            SQLExpr sqlExpr = entry.getKey().clone();
                            SQLSelectItem sqlSelectItem = new SQLSelectItem(sqlExpr);
                            sqlExpr.setParent(sqlSelectItem);
                            sqlSelectItem.setParent(x);
                            x.getSelectList().add(sqlSelectItem);
                        }
                    }
                }
            }
        }
        super.endVisit(x);
    }


    @Override
    public void endVisit(SQLIdentifierExpr x) {
        collectColumn(x);
        super.endVisit(x);
    }

    @Override
    public void endVisit(SQLPropertyExpr x) {
        collectColumn(x);
        super.endVisit(x);
    }

    private void collectColumn(SQLExpr sqlExpr) {
        SQLColumnDefinition resolvedColumn = null;
        SQLTableSource resolvedTableSource = null;
        if (sqlExpr instanceof SQLIdentifierExpr) {
            resolvedColumn = ((SQLIdentifierExpr) sqlExpr).getResolvedColumn();
            resolvedTableSource = ((SQLIdentifierExpr) sqlExpr).getResolvedTableSource();
        } else {
            resolvedColumn = ((SQLPropertyExpr) sqlExpr).getResolvedColumn();
            resolvedTableSource = ((SQLPropertyExpr) sqlExpr).getResolvedTableSource();
        }
        Map<SQLExpr, SQLColumnDefinition> selectColumn;
        if (resolvedTableSource != null && resolvedColumn != null) {
            selectColumn = tableSourceColumnMap.computeIfAbsent(resolvedTableSource, k -> new HashMap<>());
            selectColumn.put(sqlExpr, resolvedColumn);
        }
    }

    public Map<SQLTableSource, Map<SQLExpr, SQLColumnDefinition>> getTableSourceColumnMap() {
        return tableSourceColumnMap;
    }

}