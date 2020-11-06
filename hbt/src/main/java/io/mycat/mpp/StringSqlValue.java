package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.runtime.Type;

public class StringSqlValue implements SqlValue {
    final String value;

    public StringSqlValue(String value) {
        this.value = value;
    }

    public static  StringSqlValue create(String value){
        return new StringSqlValue(value);
    }

    @Override
    public Object getValue(io.mycat.mpp.plan.RowType type, DataAccessor dataAccessor, DataContext context) {
        return value;
    }


    @Override
    public Type getType() {
        return Type.of(Type.VARCHAR,false);
    }

    @Override
    public SQLObject toParseTree() {
        return SQLExprUtils.fromJavaObject(value);
    }
}