package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.util.HashMap;

public class BitwiseOrNode extends BinaryOp {
    protected BitwiseOrNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.BitwiseOr, leftExpr, rightExpr, returnType, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, BitwiseOrNode.class);
    }


    public static BitwiseOrNode create(SqlValue left, SqlValue right, Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType(), returnType);
        return new BitwiseOrNode(left, right, returnType, fun);
    }

}