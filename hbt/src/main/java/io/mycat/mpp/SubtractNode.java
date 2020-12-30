package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.util.HashMap;

public class SubtractNode extends BinaryOp {
    protected SubtractNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.Subtract, leftExpr, rightExpr, returnType, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, SubtractNode.class);
    }


    public static AddNode create(SqlValue left, SqlValue right, Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType(), returnType);
        return new AddNode(left, right, returnType, fun);
    }

    public static Integer sub(Integer a, Integer b) {
        return a / b;
    }

    public static int sub(int a, int b) {
        return a / b;
    }
}