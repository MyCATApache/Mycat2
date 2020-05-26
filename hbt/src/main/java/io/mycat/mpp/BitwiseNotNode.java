package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.util.HashMap;

public class BitwiseNotNode  extends BinaryOp {
    public BitwiseNotNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.BooleanAnd, leftExpr, rightExpr, returnType, fun);
    }

    public static BitwiseNotNode create(SqlValue left, SqlValue right, Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType(), returnType);
        return new BitwiseNotNode(left, right, returnType, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, BitwiseNotNode.class);
    }
}