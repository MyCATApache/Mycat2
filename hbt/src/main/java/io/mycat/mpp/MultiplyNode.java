package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.util.HashMap;

public class MultiplyNode extends BinaryOp {
    protected MultiplyNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.Multiply, leftExpr, rightExpr, returnType, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, MultiplyNode.class);
    }


    public static MultiplyNode create(SqlValue left, SqlValue right, Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType(), returnType);
        return new MultiplyNode(left, right, returnType, fun);
    }

    public static Integer multiply(Integer a, Integer b) {
        return a * b;
    }

    public static int multiply(int a, int b) {
        return a * b;
    }
}