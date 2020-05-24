package io.mycat.mpp;

import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.Type;

import java.util.Objects;

public class EqualityBinaryOp extends BinaryOp {
    public EqualityBinaryOp(char op, SqlValue left, SqlValue right) {
        super(left, right, op);
    }

    public static EqualityBinaryOp of(SqlValue left, SqlValue right) {
        return new EqualityBinaryOp('=', left, right);
    }

    @Override
    public Object getValue(Type type, DataAccessor dataAccessor, DataContext context) {
        return super.getValue(type, dataAccessor, context);
    }

    @Override
    public boolean getValueAsBoolean(Type columns, DataAccessor dataAccessor, DataContext dataContext) {
        Object left = this.left.getValue(columns, dataAccessor, dataContext);
        Object right = this.right.getValue(columns, dataAccessor, dataContext);
        return Objects.equals(left,right);
    }
}