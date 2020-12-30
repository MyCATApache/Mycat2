package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class IsNode extends BooleanBinaryOp {
    public IsNode(SqlValue leftExpr, SqlValue rightExpr, Invoker fun) {
        super(SQLBinaryOperator.Is, leftExpr, rightExpr, fun);
    }

    public static IsNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new IsNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, IsNode.class);
    }
}
