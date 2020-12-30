package io.mycat.mpp;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

public class BinarySqlValue implements SqlValue {
    final String value;
    public BinarySqlValue(String value) {
        this.value = value;
    }
    public static  BinarySqlValue create(String value) {
     return new BinarySqlValue(value);
    }
    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
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