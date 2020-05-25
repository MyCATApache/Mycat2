package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.plan.RowType;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;
import lombok.SneakyThrows;

import java.util.HashMap;

public class EqualityBinaryOp extends BinaryOp {
    public EqualityBinaryOp(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.Equality, left, right, Type.of(Type.INT, false), fun);
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

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, EqualityBinaryOp.class);
    }


    public static EqualityBinaryOp create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new EqualityBinaryOp(left, right, fun);
    }

    public static boolean eq(int a, int b) {
        return a == b;
    }
    public static boolean eq(Integer a, Integer b) {
        return a .equals( b);
    }

}