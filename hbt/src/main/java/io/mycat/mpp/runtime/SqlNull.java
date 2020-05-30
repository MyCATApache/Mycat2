package io.mycat.mpp.runtime;

import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import io.mycat.mpp.DataContext;
import io.mycat.mpp.SqlValue;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;

public final class SqlNull implements SqlValue {
    public static final SqlNull SINGLETON = new SqlNull();

    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return SINGLETON;
    }

    @Override
    public Type getType() {
        return Type.of(Type.NULL,true);
    }

    @Override
    public SQLObject toParseTree() {
        return SQLExprUtils.fromJavaObject(null);
    }
}