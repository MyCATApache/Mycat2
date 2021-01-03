package io.mycat.mpp;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLNotExpr;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

public class NotNode implements SqlValue {
    final SqlValue expr;

    public NotNode(SqlValue expr) {
        this.expr = expr;
    }

    public static NotNode create(SqlValue expr){
        return new NotNode(expr);
    }

    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return getValueAsBoolean(type, dataAccessor, context) ? 1 : 0;
    }

    @Override
    public boolean getValueAsBoolean(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
        Number value = (Number) expr.getValue(columns, dataAccessor, dataContext);
        return !(value.longValue() > 0);
    }

    @Override
    public Type getType() {
        return expr.getType();
    }

    @Override
    public SQLObject toParseTree() {
        return new SQLNotExpr((SQLExpr) expr.toParseTree());
    }
}