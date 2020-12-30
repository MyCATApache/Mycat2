package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.util.HashMap;

public class DivideNode extends BinaryOp {
    protected DivideNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.Divide, leftExpr, rightExpr, returnType, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, DivideNode.class);
    }


    public static DivideNode create(SqlValue left, SqlValue right, Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType(), returnType);
        return new DivideNode(left, right, returnType, fun);
    }

    public static Integer div(Integer a, Integer b) {
        return a / b;
    }

    public static int div(int a, int b) {
        return a /b;
    }
}