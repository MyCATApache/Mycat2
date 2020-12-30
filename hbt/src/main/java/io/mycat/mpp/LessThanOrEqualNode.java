package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class LessThanOrEqualNode extends BooleanBinaryOp {
    public LessThanOrEqualNode(SqlValue leftExpr, SqlValue rightExpr, Invoker fun) {
        super(SQLBinaryOperator.LessThanOrEqual, leftExpr, rightExpr, fun);
    }

    public static LessThanOrEqualNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new LessThanOrEqualNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, LessThanOrEqualNode.class);
    }
}
