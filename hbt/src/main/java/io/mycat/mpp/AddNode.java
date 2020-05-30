package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;
import io.mycat.mpp.runtime.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;

public class AddNode extends BinaryOp {
    protected AddNode(SqlValue leftExpr, SqlValue rightExpr, Type returnType, Invoker fun) {
        super(SQLBinaryOperator.Add, leftExpr, rightExpr, returnType, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, AddNode.class);
    }


    public static AddNode create(SqlValue left, SqlValue right, Type returnType) {
        Invoker fun = Ops.resolve(hashMap, left.getType(), right.getType(), returnType);
        return new AddNode(left, right, returnType, fun);
    }

    public static byte add(byte a, byte b) {
        return (byte) (((int) a) + ((int) b));
    }

    public static Byte add(Byte a, Byte b) {
        return (byte) (a.intValue() + b.intValue());
    }

    public static char add(char a, char b) {
        return (char) (a + b);
    }

    public static Character add(Character a, Character b) {
        return (char) (a + b);
    }

    public static String add(String a, String b) {
        return a + b;
    }

    public static int add(int a, int b) {
        return a + b;
    }

    public static Integer add(Integer a, Integer b) {
        return a + b;
    }

    public static long add(long a, long b) {
        return a + b;
    }

    public static Long add(Long a, Long b) {
        return a + b;
    }

    public static float add(float a, float b) {
        return a + b;
    }

    public static Float add(Float a, Float b) {
        return a + b;
    }

    public static double add(double a, double b) {
        return a + b;
    }

    public static double add(Double a, Double b) {
        return a + b;
    }

    public static BigDecimal add(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(b);
    }

    public static BigInteger add(BigInteger a, BigInteger b) {
        if (a == null || b == null) {
            return null;
        }
        return a.add(b);
    }

}