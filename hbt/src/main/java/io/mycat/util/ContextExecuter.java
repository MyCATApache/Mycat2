package io.mycat.util;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.SQLReplaceable;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@AllArgsConstructor
public class ContextExecuter extends MySqlASTVisitorAdapter {
    private final SQLContext context;

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        List<SQLExpr> arguments = x.getArguments();
        String methodName = x.getMethodName();
        SQLObject parent = x.getParent();
        if (parent instanceof SQLReplaceable) {
            Map<String, MySQLFunction> functions = context.functions();
            if (functions != null) {
                MySQLFunction mySQLFunction = functions.get(methodName);
                if (mySQLFunction != null) {
                    String[] strings = arguments.stream().map(i -> SQLUtils.normalize(Objects.toString(i))).toArray(i -> new String[i]);
                    Object res = mySQLFunction.eval(context,strings);
                    SQLExpr sqlExpr = SQLExprUtils.fromJavaObject(res);
                    sqlExpr.setParent(parent);
                    ((SQLReplaceable) parent).replace(x, sqlExpr);

                    try {
                        if (parent instanceof SQLSelectItem) {
                            ((SQLSelectItem) parent).setAlias(x.toString());
                        }
                    } catch (Throwable ignored) {

                    }
                }
            }
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        String schema = x.getSchema();
        if (schema == null) {
            x.setSchema(context.getDefaultSchema());
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLPropertyExpr) {
            SQLObject replacePointer = parent.getParent();
            if (replacePointer instanceof SQLReplaceable) {
                String alias = replacePointer.toString();
                Object sqlVariantRef = context.getSQLVariantRef(parent.toString().toLowerCase());
                if (sqlVariantRef != null) {
                    SQLExpr sqlExpr = SQLExprUtils.fromJavaObject(sqlVariantRef);
                    sqlExpr.setParent(parent);
                    ((SQLReplaceable) replacePointer).replace((SQLPropertyExpr) parent, sqlExpr);
                    try {
                        if (replacePointer instanceof SQLSelectItem) {
                            ((SQLSelectItem) replacePointer).setAlias(alias);
                        }
                    } catch (Throwable ignored) {

                    }
                }
            }
        } else if (parent instanceof SQLReplaceable) {
            Object sqlVariantRef = context.getSQLVariantRef(x.toString().toLowerCase());
            if (sqlVariantRef != null) {
                SQLExpr sqlExpr = SQLExprUtils.fromJavaObject(sqlVariantRef);
                sqlExpr.setParent(parent);
                ((SQLReplaceable) parent).replace(x, sqlExpr);
            }
        }
        return super.visit(x);
    }
}