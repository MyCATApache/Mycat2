package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.math.BigInteger;
import java.util.HashMap;

public class ModNode extends BinaryOp {
    public ModNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.Mod, leftExpr, rightExpr, returnType, fun);
    }
    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, ModNode.class);
    }

    public static ModNode create(SqlValue left, SqlValue right, Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType(),returnType);
        return new ModNode(left, right,returnType, fun);
    }

    public static int mod(int a,int b){
        return a/b;
    }
    public static Integer mod(Integer a,Integer b){
        return a/b;
    }
    public static Double mod(Double a,Double b){
        return a/b;
    }
    public static BigInteger mod(BigInteger a, BigInteger b){
        return a.mod(b);
    }
}