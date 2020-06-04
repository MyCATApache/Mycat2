package io.mycat.route;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.*;
import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.repository.SchemaResolveVisitor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.metadata.MetadataManager;

import java.util.ArrayList;
import java.util.List;

public class RouteService {
    final MetadataManager metadataManager;

    public RouteService(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    static class Scope {

        public Scope gotoSubScope() {
            return null;
        }

        public void setTableSource(Scope subScope) {

        }

        public void leaveScope() {

        }
    }

    static class ReturnObject {

    }

    public static RouteService create(MetadataManager metadataManager) {
        return new RouteService(metadataManager);
    }


    public Schema route(String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);

        return null;
    }
//
//    private ReturnObject parse(SQLStatement sqlStatement, Scope scope) {
//        if (sqlStatement instanceof SQLSelectStatement) {
//            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
//            SQLSelect select = sqlSelectStatement.getSelect();
//
//            SQLWithSubqueryClause withSubQuery = select.getWithSubQuery();
//            unsupportOperation(withSubQuery);
//
//            MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) select.getQuery();
//            ReturnObject queryReturnObject = parse(query, scope);
//            SQLOrderBy orderBy = select.getOrderBy();
//            unsupportOperation(orderBy);
//
//            SQLLimit limit = select.getLimit();
//            unsupportOperation(limit);
//
//            List<SQLHint> hints = select.getHints();
//
//            SQLObject restriction = select.getRestriction();
//            unsupportOperation(restriction);
//
//            boolean forBrowse = select.isForBrowse();
//            List<String> forXmlOptions = select.getForXmlOptions();
//            SQLExpr xmlPath = select.getXmlPath();
//
//            SQLExpr rowCount = select.getRowCount();
//            unsupportOperation(rowCount);
//            SQLExpr offset = select.getOffset();
//            unsupportOperation(offset);
//
//            System.out.println();
//        }
//        return null;
//    }
//
//    private ReturnObject parse(SQLTableSource query, Scope subScope) {
//
//    }
//
//    private ReturnObject parse(MySqlSelectQueryBlock query, Scope scope) {
//        scope = scope.gotoSubScope();
//        try {
//            SQLTableSource from = query.getFrom();
//
//            if (from != null) {
//                scope.setTableSource(scope);
//                ReturnObject returnObject = parse(from, scope);
//            }
//
//            List<SQLSelectItem> selectList = query.getSelectList();
//
//            List<SQLSelectItem> columns = new ArrayList<SQLSelectItem>();
//            for (int i = selectList.size() - 1; i >= 0; i--) {
//                SQLSelectItem selectItem = selectList.get(i);
//                SQLExpr expr = selectItem.getExpr();
//                if (expr instanceof SQLAllColumnExpr) {
//                    SQLAllColumnExpr allColumnExpr = (SQLAllColumnExpr) expr;
//                    allColumnExpr.setResolvedTableSource(from);
//
//                    visitor.visit(allColumnExpr);
//
//                    if (visitor.isEnabled(SchemaResolveVisitor.Option.ResolveAllColumn)) {
//                        extractColumns(visitor, from, null, columns);
//                    }
//                } else if (expr instanceof SQLPropertyExpr) {
//                    SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;
//                    visitor.visit(propertyExpr);
//
//                    String ownerName = propertyExpr.getOwnernName();
//                    if (propertyExpr.getName().equals("*")) {
//                        if (visitor.isEnabled(SchemaResolveVisitor.Option.ResolveAllColumn)) {
//                            SQLTableSource tableSource = x.findTableSource(ownerName);
//                            extractColumns(visitor, tableSource, ownerName, columns);
//                        }
//                    }
//
//                    SQLColumnDefinition column = propertyExpr.getResolvedColumn();
//                    if (column != null) {
//                        continue;
//                    }
//                    SQLTableSource tableSource = x.findTableSource(propertyExpr.getOwnernName());
//                    if (tableSource != null) {
//                        column = tableSource.findColumn(propertyExpr.nameHashCode64());
//                        if (column != null) {
//                            propertyExpr.setResolvedColumn(column);
//                        }
//                    }
//                } else if (expr instanceof SQLIdentifierExpr) {
//                    SQLIdentifierExpr identExpr = (SQLIdentifierExpr) expr;
//                    visitor.visit(identExpr);
//
//                    long name_hash = identExpr.nameHashCode64();
//
//                    SQLColumnDefinition column = identExpr.getResolvedColumn();
//                    if (column != null) {
//                        continue;
//                    }
//                    if (from == null) {
//                        continue;
//                    }
//                    column = from.findColumn(name_hash);
//                    if (column != null) {
//                        identExpr.setResolvedColumn(column);
//                    }
//                } else {
//                    expr.accept(visitor);
//                }
//
//                if (columns.size() > 0) {
//                    for (SQLSelectItem column : columns) {
//                        column.setParent(x);
//                        column.getExpr().accept(visitor);
//                    }
//
//                    selectList.remove(i);
//                    selectList.addAll(i, columns);
//                    columns.clear();
//                }
//            }
//
//            SQLExprTableSource into = query.getInto();
//            if (into != null) {
//                visitor.visit(into);
//            }
//
//            SQLExpr where = query.getWhere();
//            if (where != null) {
//                if (where instanceof SQLBinaryOpExpr) {
//                    SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) where;
//                    resolveExpr(visitor, binaryOpExpr.getLeft());
//                    resolveExpr(visitor, binaryOpExpr.getRight());
//                } else if (where instanceof SQLBinaryOpExprGroup) {
//                    SQLBinaryOpExprGroup binaryOpExprGroup = (SQLBinaryOpExprGroup) where;
//                    for (SQLExpr item : binaryOpExprGroup.getItems()) {
//                        if (item instanceof SQLBinaryOpExpr) {
//                            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) item;
//                            resolveExpr(visitor, binaryOpExpr.getLeft());
//                            resolveExpr(visitor, binaryOpExpr.getRight());
//                        } else {
//                            item.accept(visitor);
//                        }
//                    }
//                } else {
//                    where.accept(visitor);
//                }
//            }
//
//            SQLExpr startWith = query.getStartWith();
//            if (startWith != null) {
//                startWith.accept(visitor);
//            }
//
//            SQLExpr connectBy = query.getConnectBy();
//            if (connectBy != null) {
//                connectBy.accept(visitor);
//            }
//
//            SQLSelectGroupByClause groupBy = query.getGroupBy();
//            if (groupBy != null) {
//                groupBy.accept(visitor);
//            }
//
//            List<SQLWindow> windows = query.getWindows();
//            if (windows != null) {
//                for (SQLWindow window : windows) {
//                    window.accept(visitor);
//                }
//            }
//
//            SQLOrderBy orderBy = query.getOrderBy();
//            if (orderBy != null) {
//                for (SQLSelectOrderByItem orderByItem : orderBy.getItems()) {
//                    SQLExpr orderByItemExpr = orderByItem.getExpr();
//
//                    if (orderByItemExpr instanceof SQLIdentifierExpr) {
//                        SQLIdentifierExpr orderByItemIdentExpr = (SQLIdentifierExpr) orderByItemExpr;
//                        long hash = orderByItemIdentExpr.nameHashCode64();
//                        SQLSelectItem selectItem = x.findSelectItem(hash);
//
//                        if (selectItem != null) {
//                            orderByItem.setResolvedSelectItem(selectItem);
//
//                            SQLExpr selectItemExpr = selectItem.getExpr();
//                            if (selectItemExpr instanceof SQLIdentifierExpr) {
//                                orderByItemIdentExpr.setResolvedTableSource(((SQLIdentifierExpr) selectItemExpr).getResolvedTableSource());
//                                orderByItemIdentExpr.setResolvedColumn(((SQLIdentifierExpr) selectItemExpr).getResolvedColumn());
//                            } else if (selectItemExpr instanceof SQLPropertyExpr) {
//                                orderByItemIdentExpr.setResolvedTableSource(((SQLPropertyExpr) selectItemExpr).getResolvedTableSource());
//                                orderByItemIdentExpr.setResolvedColumn(((SQLPropertyExpr) selectItemExpr).getResolvedColumn());
//                            }
//                            continue;
//                        }
//                    }
//
//                    orderByItemExpr.accept(visitor);
//                }
//            }
//
//            int forUpdateOfSize = x.getForUpdateOfSize();
//            if (forUpdateOfSize > 0) {
//                for (SQLExpr sqlExpr : x.getForUpdateOf()) {
//                    sqlExpr.accept(visitor);
//                }
//            }
//
//            List<SQLSelectOrderByItem> distributeBy = x.getDistributeBy();
//            if (distributeBy != null) {
//                for (SQLSelectOrderByItem item : distributeBy) {
//                    item.accept(visitor);
//                }
//            }
//
//            List<SQLSelectOrderByItem> sortBy = x.getSortBy();
//            if (sortBy != null) {
//                for (SQLSelectOrderByItem item : sortBy) {
//                    item.accept(visitor);
//                }
//            }
//
//            scope.leaveScope();
//            return null;
//        }
//
//        private void unsupportOperation (SQLObject orderBy){
//            if (orderBy != null) {
//                throw new UnsupportedOperationException(orderBy.toString());
//            }
//        }
//
//
//    }

}