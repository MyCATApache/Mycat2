package io.mycat.route;

import com.alibaba.fastsql.sql.ast.SQLAdhocTableSource;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class Scope extends MySqlASTVisitorAdapter {
    final Map<String, SQLObject> scope = new HashMap<>();
    final LinkedList<SQLObject> unnames = new LinkedList<>();
    final String defaultSchema;

    public Scope(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public Scope build() {
//
//        while (!unnames.isEmpty()) {
//            SQLObject sqlObject = unnames.removeFirst();
//            int i = 0;
//            while (true) {
//                String name = "$" + i;
//                if (sqlObject instanceof SQLTableSource) {
//                    if (!scope.containsKey(name)) {
//                        ((SQLTableSource) sqlObject).setAlias(name);
//                        scope.put(name, sqlObject);
//                        break;
//                    }
//                }
//                i++;
//            }
//        }
        return this;
    }

    @Override
    public boolean visit(SQLAdhocTableSource x) {
        innerVisit(x);
        return super.visit(x);
    }

    private void innerVisit(SQLTableSource x) {
        String alias = x.getAlias();
        if (x.getAlias() == null) {
            alias = x.computeAlias();
        }
        if (alias == null) {
            unnames.add(x);
        } else {
            SQLObject pre = scope.put(alias, x);
            if (pre != null) {
                throw new UnsupportedOperationException("歧义的别名:" + alias);
            }
        }
    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        innerVisit(x);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLLateralViewTableSource x) {
        innerVisit(x);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLSubqueryTableSource x) {
        innerVisit(x);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLUnionQueryTableSource x) {
        innerVisit(x);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLUnnestTableSource x) {
        innerVisit(x);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLValuesTableSource x) {
        innerVisit(x);
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        if (x.getExpr() instanceof SQLName) {
            if (x.getSchema() == null) {
                x.setSchema(defaultSchema);
            }
            String alias = x.getAlias();
            if (x.getAlias() == null) {
                alias = x.computeAlias();
            }
            SQLObject pre = scope.put(alias, x);
            if (pre != null) {
                throw new UnsupportedOperationException("歧义的别名:" + alias);
            }
            return false;
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        String alias = x.getAlias();
        if (alias == null) {
            alias = x.computeAlias();
        }
        if (alias == null) {
            alias = x.getExpr().toString();
        }
        SQLDataType sqlDataType = x.computeDataType();
        SQLObject pre = scope.put(alias, x.getExpr());
        if (pre != null) {
            throw new UnsupportedOperationException("歧义的别名:" + alias);
        }
        return super.visit(x);
    }
}