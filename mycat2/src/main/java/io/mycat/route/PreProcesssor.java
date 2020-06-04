package io.mycat.route;

import com.alibaba.fastsql.sql.ast.*;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.*;

public class PreProcesssor extends MySqlASTVisitorAdapter {
    final String defaultSchema;

    public PreProcesssor(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    final List<SQLExprTableSource> leftTables = new ArrayList<>(2);
    final List<SQLTableSource> intermediateTables = new ArrayList<>(1);
    final LinkedList<SQLTableSource> stack = new LinkedList<SQLTableSource>();
    final Set<String> aliasSet = new HashSet<>();

    public boolean visit(SQLPropertyExpr x) {
        if (x.getOwner() instanceof SQLIdentifierExpr) {
            String name = x.getName();
            return false;
        }
        if (x.getOwner() instanceof SQLPropertyExpr) {
            String name = x.getName();
            return false;
        }
        return super.visit(x);
    }

    @Override
    public void endVisit(SQLPropertyExpr x) {
        super.endVisit(x);
    }
//    @Override
//    public void endVisit(SQLIdentifierExpr x) {
//        SQLExprTableSource peek = stack.peek();
//        if (peek != null) {
//            SQLColumnDefinition columnDefinition =  peek.findColumn(x.normalizedName());
//        }
//        super.endVisit(x);
//    }

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        SQLTableSource peek = stack.peek();
        if (peek != null) {
            SQLPropertyExpr propertyExpr = new SQLPropertyExpr(peek.computeAlias(), x.normalizedName());
            replaceInParent(x, propertyExpr, x.getParent());
        }
        return false;
    }

    private void replaceInParent(SQLExpr origin, SQLExpr target, SQLObject parent) {
        if (parent instanceof SQLReplaceable) {
            ((SQLReplaceable) parent).replace(origin, target);
        }
    }

    @Override
    public void endVisit(SQLIdentifierExpr x) {
        SQLTableSource peek = stack.peek();
        if (peek != null) {
//            SQLColumnDefinition columnDefinition =  peek.findColumn(x);
        }
        super.endVisit(x);
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        boolean b = x.getExpr() instanceof SQLName;
        if (b) {
            addAlias(x);
            leftTables.add(x);
            stack.push(x);
            return false;
        }
        return super.visit(x);
    }

    private void addAlias(SQLTableSource x) {
        if (x instanceof SQLExprTableSource) {
            SQLExprTableSource x1 = (SQLExprTableSource) x;
            if (x1.getSchema() == null) {
                x1.setSchema(defaultSchema);
            }
        }
        if (x.getAlias() == null) {
            String s = x.computeAlias();
            int i = 0;
            while (true) {
                if (aliasSet.add(s)) {
                    x.setAlias(s);
                    break;
                } else {
                    s = s + i;
                }
            }
        }
    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        SQLTableSource left = x.getLeft();
        addAlias(left);
        SQLTableSource right = x.getRight();
        addAlias(right);
        stack.push(left);
        stack.push(right);
        intermediateTables.add(x);
        SQLJoinTableSource.JoinType joinType = x.getJoinType();
        SQLExpr condition = x.getCondition();
        if (condition != null) {
            condition.accept(this);
        }
        List<SQLExpr> using = x.getUsing();
        if (using != null) {
            for (SQLExpr sqlExpr : using) {
                sqlExpr.accept(this);
            }
        }
        return false;
    }

    @Override
    public void endVisit(SQLExprTableSource x) {
        super.endVisit(x);
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        if (x.getAlias() == null) {
            String s = x.toString();
            x.setAlias(s);
            if (!aliasSet.add(s)){
                throw new UnsupportedOperationException();
            }
        } else {
            if (!aliasSet.add(x.computeAlias())) {
                throw new UnsupportedOperationException();
            }
        }
        return super.visit(x);
    }

    //从MySqlSelectQueryBlock拷贝代码,改变遍历顺序
    @Override
    public boolean visit(MySqlSelectQueryBlock x) {

        List<SQLSelectItem> selectList = x.getSelectList();
        SQLTableSource from = x.getFrom();
        SQLExprTableSource into = x.getInto();
        SQLExpr where = x.getWhere();
        SQLExpr startWith = x.getStartWith();
        SQLExpr connectBy = x.getConnectBy();
        SQLSelectGroupByClause groupBy = x.getGroupBy();
        SQLOrderBy orderBy = x.getOrderBy();
        List<SQLWindow> windows = x.getWindows();
        List<SQLSelectOrderByItem> distributeBy = x.getDistributeBy();
        SQLLimit limit = x.getLimit();

        if (from != null) {
            from.accept(this);
        }

        for (int i = 0; i < selectList.size(); i++) {
            SQLSelectItem item = selectList.get(i);
            if (item != null) {
                item.accept(this);
            }
        }

        if (into != null) {
            throw new UnsupportedOperationException();
        }

        if (where != null) {
            where.accept(this);
        }

        if (groupBy != null) {
            groupBy.accept(this);
        }


        if (orderBy != null) {
            orderBy.accept(this);
        }


        if (limit != null) {
            limit.accept(this);
        }

        if (from != null) {
            stack.pop();
        }

        return false;
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {
        super.endVisit(x);
    }

}