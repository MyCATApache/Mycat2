package io.mycat.mpp;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.mpp.runtime.Invoker;
import io.mycat.mpp.runtime.Ops;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

public class EqualityBinaryOp extends BooleanBinaryOp {

    protected EqualityBinaryOp(SqlValue left, SqlValue right, Invoker fun) {
        super(SQLBinaryOperator.Equality, left, right, fun);
    }

    public static EqualityBinaryOp create(SqlValue left, SqlValue right) {
        Invoker fun = Ops.resolveReturnBoolean(hashMap, left.getType(), right.getType());
        return new EqualityBinaryOp(left, right, fun);
    }

    final static HashMap<Ops.Key, Object> hashMap = new HashMap<>();

    static {
        Ops.registed(hashMap, EqualityBinaryOp.class);
    }


    public static boolean eq(byte a, byte b) {
        return a == b;
    }

    public static boolean eq(Byte a, Byte b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(short a, short b) {
        return a == b;
    }

    public static boolean eq(Short a, Short b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(char a, char b) {
        return a == b;
    }

    public static boolean eq(Character a, Character b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(int a, int b) {
        return a == b;
    }

    public static boolean eq(Integer a, Integer b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(long a, long b) {
        return a == b;
    }

    public static boolean eq(Long a, Long b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(float a, float b) {
        return Float.compare(a, b) == 0;
    }

    public static boolean eq(Float a, Float b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(double a, double b) {
        return Double.compare(a, b) == 0;
    }

    public static boolean eq(Double a, Double b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(BigInteger a, BigInteger b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(BigDecimal a, BigDecimal b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(Time a, Time b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(Date a, Date b) {
        return Objects.equals(a, b);
    }

    public static boolean eq(byte[] a, byte[] b) {
        return Arrays.equals(a, b);
    }

    public static boolean eq(Timestamp a, Timestamp b) {
        return Objects.equals(a, b);
    }
}