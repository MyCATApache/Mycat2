package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.util.HashMap;

public class RightShiftNode extends BinaryOp {
    public RightShiftNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.RightShift, leftExpr, rightExpr,returnType, fun);
    }

    public static RightShiftNode create(SqlValue left, SqlValue right,Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType());
        return new RightShiftNode(left, right,returnType, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, RightShiftNode.class);
    }
}
