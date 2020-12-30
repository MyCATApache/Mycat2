package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class GreaterThanNode  extends BooleanBinaryOp {
    public GreaterThanNode(SqlValue leftExpr, SqlValue rightExpr, Invoker fun) {
        super(SQLBinaryOperator.GreaterThan, leftExpr, rightExpr, fun);
    }

    public static GreaterThanNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new GreaterThanNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, GreaterThanNode.class);
    }
}
