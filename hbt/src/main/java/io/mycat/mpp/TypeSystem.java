package io.mycat.mpp;

import io.mycat.mpp.runtime.SqlNull;
import io.mycat.mpp.runtime.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.function.Function;

public class TypeSystem {
    public static SqlNull NULL = SqlNull.SINGLETON;
    public static final int INT = 1;
    public static final int VARCHAR = 2;
    public static final int DECIMAL = 3;
    public static final int BOOL = 4;
    public static final int TIMESTAMP = 5;
    public static final int DATETIME = 6;

    public static boolean isSqlNull(Object value) {
        return value == SqlNull.SINGLETON;
    }

    public static SqlNull asSqlNull(Object value) {
        assert isSqlNull(value);
        return SqlNull.SINGLETON;
    }

    public static interface ValueObject {
        Object apply(Object input);

        boolean wasNull();
    }

    public static interface NonNullValueObject extends ValueObject {
        Object apply(Object input);

        @Override
        default boolean wasNull() {
            return false;
        }
    }

    public static interface NullableValueObject extends ValueObject {
        Object apply(Object input);

        @Override
        default boolean wasNull() {
            return true;
        }
    }

    /**
     * Implicit type conversion also takes place on dyadic arithmetic operations (+,-,*,/). MariaDB chooses the minimum data type that is guaranteed to fit the result and converts both arguments to the result data type.
     *
     * @param origin
     * @param target
     * @return
     */
    public static Type resolveArithmeticType(Type origin, Type target) {
        if (origin.isDecimal() || target.isDecimal()) {
            return Type.of(Type.DECIMAL, origin.isNullable() || target.isNullable());
        }
        if (origin.isString() || target.isString()) {
            return Type.of(Type.DECIMAL, origin.isNullable() || target.isNullable());
        }
        if (origin.isDecimal() || target.isDecimal()) {
            return Type.of(Type.DECIMAL, origin.isNullable() || target.isNullable());
        }
        return Type.of(Type.INT, origin.isNullable() || target.isNullable());
    }

    public static Function<?, ?> convert(Type origin, Type target) {
        if (origin.equals(target)) {
            return Function.identity();
        }
        if (origin.getBase() == target.getBase()) {
            if (origin.isNullable() && target.isNullable()) {
                return Function.identity();
            }
            if (!origin.isNullable() && !target.isNullable()) {
                return Function.identity();
            }
            if (!origin.isNullable() && target.isNullable()) {
                return Function.identity();
            }
            if (origin.isNullable() && !target.isNullable()) {
                throw new IllegalArgumentException();
            }
        }
        int leftType = origin.getBase();
        int rightType = target.getBase();
        switch (leftType) {
            case Type.NULL:{
                return Function.identity();
            }
            case Type.INT: {
                switch (rightType) {
                    case Type.INT: {
                        return Function.identity();
                    }
                    case Type.VARCHAR: {
                        return (Function<Integer, String>) o -> {
                            if (o == null) return null;
                            return o.toString();
                        };
                    }
                    case Type.DECIMAL: {
                        return (Function<Integer, BigDecimal>) o -> {
                            if (o == null) return null;
                            return BigDecimal.valueOf(o);
                        };
                    }
                    case Type.TIMESTAMP: {
                        return (Function<Integer, Timestamp>) o -> {
                            if (o == null) return null;
                            return new Timestamp(o);
                        };
                    }
                    case Type.DATETIME: {
                        return (Function<Integer, Date>) o -> {
                            if (o == null) return null;
                            return new Date(o);
                        };
                    }
                    default:
                        throw new IllegalArgumentException();
                }
            }
            case Type.VARCHAR: {
                switch (rightType) {
                    case Type.INT: {
                        return (Function<String, Integer>) o -> {
                            if (o == null) return null;
                            return Integer.parseInt(o);
                        };
                    }
                    case Type.VARCHAR: {
                        return Function.identity();
                    }
                    case Type.DECIMAL: {
                        return (Function<String, BigDecimal>) o -> {
                            if (o == null) return null;
                            return new BigDecimal(o);
                        };
                    }
                    case Type.TIMESTAMP: {
                        return (Function<String, Timestamp>) o -> {
                            if (o == null) return null;//todo check
                            return Timestamp.valueOf(o);
                        };
                    }
                    case Type.DATETIME: {
                        return (Function<String, Date>) o -> {
                            if (o == null) return null;//todo check
                            return Date.valueOf(o);
                        };
                    }
                    default:
                        throw new IllegalArgumentException();
                }
            }
            case Type.DECIMAL: {
                switch (rightType) {
                    case Type.INT: {
                        return (Function<BigDecimal, Integer>) o -> {
                            if (o == null) return null;
                            return o.intValue();
                        };
                    }
                    case Type.VARCHAR: {
                        return (Function<BigDecimal, String>) o -> {
                            if (o == null) return null;
                            return o.toString();
                        };
                    }
                    case Type.DECIMAL: {
                        return Function.identity();
                    }
                    case Type.TIMESTAMP: {
                        return (Function<BigDecimal, Timestamp>) o -> {
                            if (o == null) return null;//todo check
                            return Timestamp.valueOf(o.toString());
                        };
                    }
                    case Type.DATETIME: {
                        return (Function<BigDecimal, Date>) o -> {
                            if (o == null) return null;//todo check
                            return Date.valueOf(o.toString());
                        };
                    }
                    default:
                        throw new IllegalArgumentException();
                }
            }
            case Type.TIMESTAMP: {
                switch (rightType) {
                    case Type.INT: {
                        return (Function<Timestamp, Integer>) o -> {
                            if (o == null) return null;
                            return (int) o.getTime();
                        };
                    }
                    case Type.VARCHAR: {
                        return (Function<Timestamp, String>) o -> {
                            if (o == null) return null;
                            return o.toString();
                        };
                    }
                    case Type.DECIMAL: {
                        return (Function<Timestamp, BigDecimal>) o -> {
                            if (o == null) return null;
                            return BigDecimal.valueOf(o.getTime());
                        };
                    }
                    case Type.TIMESTAMP: {
                        return Function.identity();
                    }
                    case Type.DATETIME: {
                        return (Function<Timestamp, Date>) o -> {
                            if (o == null) return null;
                            return new Date(o.getTime());
                        };
                    }
                    default:
                        throw new IllegalArgumentException();
                }
            }
            case Type.DATETIME: {
                switch (rightType) {
                    case Type.INT: {
                        return (Function<Date, Integer>) o -> {
                            if (o == null) return null;
                            return (int) o.getTime();
                        };
                    }
                    case Type.VARCHAR: {
                        return (Function<Date, String>) o -> {
                            if (o == null) return null;
                            return o.toString();
                        };
                    }
                    case Type.DECIMAL: {
                        return (Function<Date, BigDecimal>) o -> {
                            if (o == null) return null;
                            return BigDecimal.valueOf(o.getTime());
                        };
                    }
                    case Type.TIMESTAMP: {
                        return (Function<Timestamp, Date>) o -> {
                            if (o == null) return null;
                            return new Date(o.getTime());
                        };
                    }
                    case Type.DATETIME: {
                        return Function.identity();
                    }
                    default:
                        throw new IllegalArgumentException();
                }
            }
            default:
                throw new IllegalArgumentException();
        }
    }

    public static Type resloveType(Type leftExprType, Type rightExprType) {
        if (leftExprType.equals(rightExprType)) {
            return leftExprType;
        } else {
            if (leftExprType.isNullable() != rightExprType.isNullable()) {
                return resloveType(leftExprType.toNullable(), rightExprType.toNullable());
            }
            if (leftExprType.isInt() == rightExprType.isInt()) {
                leftExprType = rightExprType.toIntType();
            } else if (leftExprType.isString() == rightExprType.isString()) {
                leftExprType = rightExprType.toStringType();
            } else if (leftExprType.isTimestampOrDateTime() || rightExprType.isTimestampOrDateTime()) {
                leftExprType = leftExprType.toTimestamp();
            } else if (leftExprType.isDecimal() || rightExprType.isDecimal()) {
                leftExprType = rightExprType.toDecimalType();
            } else {
                leftExprType = rightExprType.toDecimalType();
            }
            return leftExprType;
        }
    }

    //////////////////////////////////////////////////////////////////////////

    public static double intToDouble(long value) {
        return value;
    }

    public static double bigIntegerToDouble(BigInteger value) {
        return value.doubleValue();
    }

    public static double longToDouble(long value) {
        return value;
    }

    public static double asDouble(Object value) {
        return (Double) value;
    }

    public static boolean isChar(Object value) {
        return value instanceof CharSequence;
    }

}