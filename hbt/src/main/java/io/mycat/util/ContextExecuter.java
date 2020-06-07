package io.mycat.util;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.SQLReplaceable;
import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.ast.statement.SQLAssignItem;
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
                    Object res = mySQLFunction.eval(context, strings);
                    SQLExpr sqlExpr;
                    if (res instanceof SQLValuableExpr){
                        sqlExpr =(SQLValuableExpr) res;
                    }else {
                        sqlExpr = SQLExprUtils.fromJavaObject(res);
                    }
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
    public boolean visit(SQLAssignItem x) {
        SQLExpr target = x.getTarget();//不处理
        SQLExpr value = x.getValue();
        value.accept(this);
        return false;//不需要再次遍历
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLAssignItem || parent.getParent() instanceof SQLAssignItem) {//set 语句不处理
            return super.visit(x);
        }
        if (parent instanceof SQLPropertyExpr) {
            SQLObject replacePointer = parent.getParent();
            if (replacePointer instanceof SQLReplaceable) {
                String alias = replacePointer.toString();
                Object sqlVariantRef = context.getSQLVariantRef(parent.toString().toLowerCase());
                if (sqlVariantRef != null) {
                    if (sqlVariantRef instanceof Boolean) {
                        if (Boolean.TRUE.equals(sqlVariantRef)) {
                            sqlVariantRef = 1;
                        } else {
                            sqlVariantRef = 0;
                        }
                    }
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
                //查询变量的替换,mycat内部保留原始名称 wangzihaogithub. 2020年5月2日19:15:12
                if(parent instanceof SQLSelectItem && ((SQLSelectItem) parent).getAlias() == null){
                    ((SQLSelectItem) parent).setAlias(x.toString());
                }
                //-----------------------
                ((SQLReplaceable) parent).replace(x, sqlExpr);
            }
        }
        return super.visit(x);
    }
}