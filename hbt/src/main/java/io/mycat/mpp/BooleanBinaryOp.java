package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Type;
import lombok.SneakyThrows;

public abstract class BooleanBinaryOp extends BinaryOp {
    public BooleanBinaryOp(SQLBinaryOperator operator, SqlValue leftExpr, SqlValue rightExpr, Invoker fun) {
        super(operator, leftExpr, rightExpr, Type.of(Type.INT, false), fun);
    }
    @Override
    public Object getValue(RowType type, DataAccessor dataAccessor, DataContext context) {
        return this.getValueAsBoolean(type, dataAccessor, context) ? 1 : 0;
    }

    @Override
    @SneakyThrows
    public boolean getValueAsBoolean(RowType columns, DataAccessor dataAccessor, DataContext dataContext) {
        return fun.invokeWithArguments(leftExpr.getValue(columns, dataAccessor, dataContext),
                rightExpr.getValue(columns, dataAccessor, dataContext)) == Boolean.TRUE;
    }
    @Override
    public Type getType() {
        return Type.of(Type.INT, false);
    }
}