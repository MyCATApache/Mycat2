package io.mycat.mpp.expr.func;

import io.mycat.mpp.NullPointer;

import java.math.BigInteger;

public class BitOps {


    /////////////////////////////////bitAnd////////////////////////////
    public static BigInteger bitAnd(BigInteger p, BigInteger p2) {
        return bitAnd(p, p2, NullPointer.DEFAULT);
    }

    public static BigInteger bitAnd(BigInteger p, BigInteger p2, NullPointer nullPointer) {
        if (p == null || p2 == null) {
            nullPointer.setNullValue(true);
            return BigInteger.ZERO;
        }
        nullPointer.setNullValue(false);
        return p.and(p2);
    }

    /////////////////////////////////bitCount////////////////////////////
    public static BigInteger bitCount(BigInteger p) {
        return bitCount(p, NullPointer.DEFAULT);
    }

    public static BigInteger bitCount(BigInteger p, NullPointer nullPointer) {
        if (p == null) {
            nullPointer.setNullValue(true);
            return BigInteger.ZERO;
        }
        nullPointer.setNullValue(false);
        return BigInteger.valueOf(p.bitCount());
    }


    /////////////////////////////////bitInversion////////////////////////////
    public static final BigInteger BI64BACK = new BigInteger("18446744073709551616");

    public static BigInteger bitInversion(BigInteger p) {
        return bitCount(p, NullPointer.DEFAULT);
    }

    public static BigInteger bitInversion(BigInteger p, NullPointer nullPointer) {
        if (p == null) {
            nullPointer.setNullValue(true);
            return BigInteger.ZERO;
        }
        nullPointer.setNullValue(false);
        int compareTo = p.compareTo(BigInteger.ZERO);
        if (compareTo > 0) {
            return BI64BACK.subtract(BigInteger.ONE).subtract(p);
        } else if (compareTo == 0) {
            return BI64BACK.subtract(BigInteger.ONE);
        } else {
            return p.negate().subtract(BigInteger.ONE);
        }
    }

    /////////////////////////////////bitOr////////////////////////////
    public static BigInteger bitOr(BigInteger p, BigInteger p2) {
        return bitOr(p, p2, NullPointer.DEFAULT);
    }

    public static BigInteger bitOr(BigInteger p, BigInteger p2, NullPointer nullPointer) {
        if (p == null || p2 == null) {
            nullPointer.setNullValue(true);
            return BigInteger.ZERO;
        }
        nullPointer.setNullValue(false);
        return p.or(p2);
    }

    /////////////////////////////////bitXor////////////////////////////
    public static BigInteger bitXor(BigInteger p, BigInteger p2) {
        return bitXor(p, p2, NullPointer.DEFAULT);
    }

    public static BigInteger bitXor(BigInteger p, BigInteger p2, NullPointer nullPointer) {
        if (p == null || p2 == null) {
            nullPointer.setNullValue(true);
            return BigInteger.ZERO;
        }
        nullPointer.setNullValue(false);
        return p.xor(p2);
    }

    /////////////////////////////////shiftLeft////////////////////////////
    public static BigInteger shiftLeft(BigInteger p,BigInteger p2) {
        return shiftLeft(p, p2,NullPointer.DEFAULT);
    }

    public static BigInteger shiftLeft(BigInteger p, BigInteger p2, NullPointer nullPointer) {
        if (p == null || p2 == null) {
            nullPointer.setNullValue(true);
            return BigInteger.ZERO;
        }
        nullPointer.setNullValue(false);
        int shift = p2.intValue();
        return shift < Long.SIZE * 8 ? p.shiftLeft(shift) : BigInteger.ZERO;
    }

    /////////////////////////////////shiftRight////////////////////////////
    public static BigInteger shiftRight(BigInteger p,BigInteger p2) {
        return shiftRight(p, p2,NullPointer.DEFAULT);
    }

    public static BigInteger shiftRight(BigInteger p, BigInteger p2, NullPointer nullPointer) {
        if (p == null || p2 == null) {
            nullPointer.setNullValue(true);
            return BigInteger.ZERO;
        }
        nullPointer.setNullValue(false);
        int shift = p2.intValue();
        return shift < Long.SIZE * 8 ? p.shiftRight(shift) : BigInteger.ZERO;
    }
}