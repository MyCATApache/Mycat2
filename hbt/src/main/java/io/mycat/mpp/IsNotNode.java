package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.util.HashMap;

public class IsNotNode  extends BooleanBinaryOp {

    protected IsNotNode(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.IsNot, left, right, fun);
    }

    public static IsNotNode create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new IsNotNode(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, IsNotNode.class);
    }

}