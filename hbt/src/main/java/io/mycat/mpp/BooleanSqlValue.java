package io.mycat.mpp;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Type;

public class BooleanSqlValue implements SqlValue {
   final Boolean value;

    public BooleanSqlValue(Boolean value) {
        this.value = value;
    }

    public static  BooleanSqlValue create(boolean value){
        return new BooleanSqlValue(value);
    }

    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return value;
    }

    @Override
    public boolean getValueAsBoolean(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
        return value;
    }

    @Override
    public Type getType() {
        return Type.of(Type.INT,false);
    }

    @Override
    public SQLObject toParseTree() {
        return SQLExprUtils.fromJavaObject(value);
    }
}