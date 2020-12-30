package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class LessThanOrGreaterNode extends BooleanBinaryOp {
    public LessThanOrGreaterNode(SqlValue leftExpr, SqlValue rightExpr, Invoker fun) {
        super(SQLBinaryOperator.LessThanOrEqualOrGreaterThan, leftExpr, rightExpr, fun);
    }

    public static LessThanOrGreaterNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new LessThanOrGreaterNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, LessThanOrGreaterNode.class);
    }
}
