package org.apache.calcite.mycat;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Types;
import org.eclipse.jetty.server.handler.ScopedHandler;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

public enum  MycatBuiltInMethod {
    STRING_TO_DATE(MycatBuiltInMethodImpl.class, "stringToDate", String.class),
    STRING_TO_TIME(MycatBuiltInMethodImpl.class, "stringToTime", String.class),
    STRING_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "stringToTimestamp", String.class),
    LONG_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "longToTimestamp", Long.class),
    LONG_TO_DATE(MycatBuiltInMethodImpl.class, "longToDate", Long.class),
    DATE_TO_BIGINT(MycatBuiltInMethodImpl.class, "dateToLong", LocalDate.class),
    TIMESTAMP_TO_DOUBLE(MycatBuiltInMethodImpl.class, "timestampToDouble", LocalDateTime.class),
    BOOLEAN_TO_TINYINT(MycatBuiltInMethodImpl.class, "booleanToTinyint", Boolean.class),
    SMALLINT_TO_TINYINT(MycatBuiltInMethodImpl.class, "smallintToTinyint", Short.class),
    INTEGER_TO_TINYINT(MycatBuiltInMethodImpl.class, "integerToTinyint", Integer.class),
    BIGINT_TO_TINYINT(MycatBuiltInMethodImpl.class, "bigintToTinyint", Long.class),
//////////////////////////////////////////////////////////////
DECIMAL_TO_TINYINT(MycatBuiltInMethodImpl.class, "decimalToTinyint", BigDecimal.class),
    FLOAT_TO_TINYINT(MycatBuiltInMethodImpl.class, "floatToTinyint", Double.class),
    REAL_TO_TINYINT(MycatBuiltInMethodImpl.class, "realToTinyint", Float.class),
    DOUBLE_TO_TINYINT(MycatBuiltInMethodImpl.class, "doubleToTinyint", Double.class),
    DATE_TO_TINYINT(MycatBuiltInMethodImpl.class, "dateToTinyint", LocalDate.class),
    TIME_TO_TINYINT(MycatBuiltInMethodImpl.class, "timeToTinyint", Duration.class),
    TIMESTAMP_TO_TINYINT(MycatBuiltInMethodImpl.class, "timestampToTinyint", LocalDateTime.class),
    PERIOD_TO_TINYINT(MycatBuiltInMethodImpl.class, "periodToTinyint", Period.class),
    DURATION_TO_TINYINT(MycatBuiltInMethodImpl.class, "durationToTinyint", Duration.class),
    STRING_TO_TINYINT(MycatBuiltInMethodImpl.class, "stringToTinyint", String.class),

    BYTESTRING_TO_TINYINT(MycatBuiltInMethodImpl.class, "byteStringToTinyint", ByteString.class),

    BOOLEAN_TO_SMALLINT(MycatBuiltInMethodImpl.class, "booleanToSmallint", Boolean.class),
    TINYINT_TO_SMALLINT(MycatBuiltInMethodImpl.class, "tinyintToSmallint", Byte.class),
    INTEGER_TO_SMALLINT(MycatBuiltInMethodImpl.class, "integerToTSmallint", Long.class),
    BIGINT_TO_SMALLINT(MycatBuiltInMethodImpl.class, "bigintToSmallint", Long.class),
    DECIMAL_TO_SMALLINT(MycatBuiltInMethodImpl.class, "bigDecimalToSmallint", BigDecimal.class),
    FLOAT_TO_SMALLINT(MycatBuiltInMethodImpl.class, "floatToSmallint", Double.class),
    REAL_TO_SMALLINT(MycatBuiltInMethodImpl.class, "realToSmallint", Float.class),
    DOUBLE_TO_SMALLINT(MycatBuiltInMethodImpl.class, "doubleToSmallint", Double.class),
    DATE_TO_SMALLINT(MycatBuiltInMethodImpl.class, "dateToSmallint", LocalDate.class),
    TIME_TO_SMALLINT(MycatBuiltInMethodImpl.class, "timeToSmallint", Duration.class),
    TIMESTAMP_TO_SMALLINT(MycatBuiltInMethodImpl.class, "timestampToSmallint", LocalDateTime.class),
    PERIOD_TO_SMALLINT(MycatBuiltInMethodImpl.class, "periodToSmallint", Period.class),
    STRING_TO_SMALLINT(MycatBuiltInMethodImpl.class, "stringToSmallint", String.class),
    BYTESTRING_TO_SMALLINT(MycatBuiltInMethodImpl.class, "byteStringToSmallint", ByteString.class),
    DURATION_TO_SMALLINT(MycatBuiltInMethodImpl.class, "durationToSmallint", Duration.class),

    SMALLINT_TO_BIGINT(MycatBuiltInMethodImpl.class, "smallintToBigint", Short.class),
    BOOLEAN_TO_BIGINT(MycatBuiltInMethodImpl.class, "booleanToBigint", Boolean.class),
    TINYINT_TO_BIGINT(MycatBuiltInMethodImpl.class, "tinyToBigint", Byte.class),
    INTEGER_TO_BIGINT(MycatBuiltInMethodImpl.class, "integerToBigint", Integer.class),
    BIGINT_TO_BIGINT(MycatBuiltInMethodImpl.class, "bigintToBigint", Long.class),
    DECIMAL_TO_BIGINT(MycatBuiltInMethodImpl.class, "decimalToBigint", BigDecimal.class),
    FLOAT_TO_BIGINT(MycatBuiltInMethodImpl.class, "floatToBigint", Double.class),
    REAL_TO_BIGINT(MycatBuiltInMethodImpl.class, "realToBigint", Float.class),
    DOUBLE_TO_BIGINT(MycatBuiltInMethodImpl.class, "doubleToBigint", Double.class),
    TIME_TO_BIGINT(MycatBuiltInMethodImpl.class, "timeToBigint", Duration.class),
    TIMESTAMP_TO_BIGINT(MycatBuiltInMethodImpl.class, "timestampToBigint", LocalDateTime.class),
    PERIOD_TO_BIGINT(MycatBuiltInMethodImpl.class, "periodToBigint", Period.class),
    DURATION_TO_BIGINT(MycatBuiltInMethodImpl.class, "durationToBigint", Duration.class),
    STRING_TO_BIGINT(MycatBuiltInMethodImpl.class, "stringToBigint", String.class),
    BYTESTRING_TO_BIGINT(MycatBuiltInMethodImpl.class, "byteStringToBigint", ByteString.class),

    SMALLINT_TO_DECIMAL(MycatBuiltInMethodImpl.class, "smallintToDecimal", Short.class),
    BOOLEAN_TO_DECIMAL(MycatBuiltInMethodImpl.class, "booleanToDecimal", Boolean.class),
    TINYINT_TO_DECIMAL(MycatBuiltInMethodImpl.class, "tinyintToDecimal", Byte.class),
    INTEGER_TO_DECIMAL(MycatBuiltInMethodImpl.class, "integerToDecimal", Long.class),
    BIGINT_TO_DECIMAL(MycatBuiltInMethodImpl.class, "bigintToDecimal", Long.class),
    DECIMAL_TO_DECIMAL(MycatBuiltInMethodImpl.class, "decimalToDecimal", BigDecimal.class),
    FLOAT_TO_DECIMAL(MycatBuiltInMethodImpl.class, "floatToDecimal", Double.class),
    REAL_TO_DECIMAL(MycatBuiltInMethodImpl.class, "realToDecimal", Float.class),
    DOUBLE_TO_DECIMAL(MycatBuiltInMethodImpl.class, "doubleToDecimal", Double.class),
    TIME_TO_DECIMAL(MycatBuiltInMethodImpl.class, "timeToDecimal", Duration.class),
    TIMESTAMP_TO_DECIMAL(MycatBuiltInMethodImpl.class, "timestampToDecimal", LocalDateTime.class),
    PERIOD_TO_DECIMAL(MycatBuiltInMethodImpl.class, "periodToDecimal", Period.class),
    DURATION_TO_DECIMAL(MycatBuiltInMethodImpl.class, "durationToDecimal", Duration.class),
    STRING_TO_DECIMAL(MycatBuiltInMethodImpl.class, "stringToDecimal", String.class),
    BYTESTRING_TO_DECIMAL(MycatBuiltInMethodImpl.class, "bytestringToDecimal", ByteString.class),

    SMALLINT_TO_FLOAT(MycatBuiltInMethodImpl.class, "smallintToFloat", Short.class),
    BOOLEAN_TO_FLOAT(MycatBuiltInMethodImpl.class, "booleanToFloat", Boolean.class),
    TINYINT_TO_FLOAT(MycatBuiltInMethodImpl.class, "tinyintToFloat", Byte.class),
    INTEGER_TO_FLOAT(MycatBuiltInMethodImpl.class, "integerToFloat", Long.class),
    BIGINT_TO_FLOAT(MycatBuiltInMethodImpl.class, "bigintToFloat", Long.class),
    DECIMAL_TO_FLOAT(MycatBuiltInMethodImpl.class, "decimalToFloat", BigDecimal.class),
    FLOAT_TO_FLOAT(MycatBuiltInMethodImpl.class, "floatToFloat", Double.class),
    REAL_TO_FLOAT(MycatBuiltInMethodImpl.class, "realToFloat", Float.class),
    DOUBLE_TO_FLOAT(MycatBuiltInMethodImpl.class, "doubleToFloat", Double.class),
    DATE_TO_FLOAT(MycatBuiltInMethodImpl.class, "dateToFloat", LocalDate.class),
    TIME_TO_FLOAT(MycatBuiltInMethodImpl.class, "timeToFloat", Duration.class),
    TIMESTAMP_TO_FLOAT(MycatBuiltInMethodImpl.class, "timestampToFloat", LocalDateTime.class),
    PERIOD_TO_FLOAT(MycatBuiltInMethodImpl.class, "periodToFloat", Period.class),
    DURATION_TO_FLOAT(MycatBuiltInMethodImpl.class, "durationToFloat", Duration.class),
    STRING_TO_FLOAT(MycatBuiltInMethodImpl.class, "stringToFloat", String.class),
    BYTESTRING_TO_FLOAT(MycatBuiltInMethodImpl.class, "byteStringToFloat", ByteString.class),

    BOOLEAN_TO_REAL(MycatBuiltInMethodImpl.class, "booleanToReal", Boolean.class),
    SMALLINT_TO_REAL(MycatBuiltInMethodImpl.class, "smallintToReal", Short.class),
    TINYINT_TO_REAL(MycatBuiltInMethodImpl.class, "tinyintToReal", Byte.class),
    INTEGER_TO_REAL(MycatBuiltInMethodImpl.class, "integerToReal", Long.class),
    BIGINT_TO_REAL(MycatBuiltInMethodImpl.class, "bigintToReal", Long.class),
    DECIMAL_TO_REAL(MycatBuiltInMethodImpl.class, "decimalToReal", BigDecimal.class),
    FLOAT_TO_REAL(MycatBuiltInMethodImpl.class, "floatToReal", Double.class),
    REAL_TO_REAL(MycatBuiltInMethodImpl.class, "realToReal", Float.class),
    DOUBLE_TO_REAL(MycatBuiltInMethodImpl.class, "doubleToReal", Double.class),
    DATE_TO_REAL(MycatBuiltInMethodImpl.class, "dateToReal", LocalDate.class),
    TIME_TO_REAL(MycatBuiltInMethodImpl.class, "timeToReal", Duration.class),
    TIMESTAMP_TO_REAL(MycatBuiltInMethodImpl.class, "timestampToReal", LocalDateTime.class),
    PERIOD_TO_REAL(MycatBuiltInMethodImpl.class, "periodToReal", Period.class),
    DURATION_TO_REAL(MycatBuiltInMethodImpl.class, "durationToReal", Duration.class),
    STRING_TO_REAL(MycatBuiltInMethodImpl.class, "stringToReal", String.class),
    BYTESTRING_TO_REAL(MycatBuiltInMethodImpl.class, "byteStringToReal", ByteString.class),

    BOOLEAN_TO_DOUBLE(MycatBuiltInMethodImpl.class, "booleanToDouble", Boolean.class),
    TINYINT_TO_DOUBLE(MycatBuiltInMethodImpl.class, "tinyintToDouble", Byte.class),
    SMALLINT_TO_DOUBLE(MycatBuiltInMethodImpl.class, "smallintToDouble", Short.class),
    INTEGER_TO_DOUBLE(MycatBuiltInMethodImpl.class, "integerToDouble", Long.class),
    BIGINT_TO_DOUBLE(MycatBuiltInMethodImpl.class, "bigintToDouble", Long.class),
    DECIMAL_TO_DOUBLE(MycatBuiltInMethodImpl.class, "decimalToDouble", BigDecimal.class),
    FLOAT_TO_DOUBLE(MycatBuiltInMethodImpl.class, "floatToDouble", Double.class),
    REAL_TO_DOUBLE(MycatBuiltInMethodImpl.class, "realToDouble", Float.class),
    DOUBLE_TO_DOUBLE(MycatBuiltInMethodImpl.class, "doubleToDouble", Double.class),
    DATE_TO_DOUBLE(MycatBuiltInMethodImpl.class, "dateToDouble", LocalDate.class),
    TIME_TO_DOUBLE(MycatBuiltInMethodImpl.class, "timeToDouble", Duration.class),
    PERIOD_TO_DOUBLE(MycatBuiltInMethodImpl.class, "periodToDouble", Period.class),
    DURATION_TO_DOUBLE(MycatBuiltInMethodImpl.class, "durationToDouble", Duration.class),
    STRING_TO_DOUBLE(MycatBuiltInMethodImpl.class, "stringToDouble", String.class),
    BYTESTRING_TO_DOUBLE(MycatBuiltInMethodImpl.class, "byteStringToDouble", ByteString.class),

    BOOLEAN_TO_DATE(MycatBuiltInMethodImpl.class, "booleanToDate", Boolean.class),
    TINYINT_TO_DATE(MycatBuiltInMethodImpl.class, "tinyintToDate", Byte.class),
    SMALLINT_TO_DATE(MycatBuiltInMethodImpl.class, "smallintToDate", Short.class),
    INTEGER_TO_DATE(MycatBuiltInMethodImpl.class, "integerToDate", Long.class),
    BIGINT_TO_DATE(MycatBuiltInMethodImpl.class, "bigintToDate", Long.class),
    DECIMAL_TO_DATE(MycatBuiltInMethodImpl.class, "decimalToDate", BigDecimal.class),
    FLOAT_TO_DATE(MycatBuiltInMethodImpl.class, "floatToDate", Double.class),
    REAL_TO_DATE(MycatBuiltInMethodImpl.class, "realToDate", Float.class),
    DOUBLE_TO_DATE(MycatBuiltInMethodImpl.class, "doubleToDate", Double.class),
    TIME_TO_DATE(MycatBuiltInMethodImpl.class, "timeToDate", Duration.class),
    TIMESTAMP_TO_DATE(MycatBuiltInMethodImpl.class, "timestampToDate", LocalDateTime.class),
    PERIOD_TO_DATE(MycatBuiltInMethodImpl.class, "periodToDate", Period.class),
    DURATION_TO_DATE(MycatBuiltInMethodImpl.class, "durationToDate", Duration.class),
    BYTESTRING_TO_DATE(MycatBuiltInMethodImpl.class, "byteStringToDate", ByteString.class),
    BOOLEAN_TO_TIME(MycatBuiltInMethodImpl.class, "booleanToDate", Boolean.class),
    TINYINT_TO_TIME(MycatBuiltInMethodImpl.class, "tinyintToDate", Byte.class),
    SMALLINT_TO_TIME(MycatBuiltInMethodImpl.class, "smallintToDate", Short.class),
    INTEGER_TO_TIME(MycatBuiltInMethodImpl.class, "integerToDate", Long.class),
    BIGINT_TO_TIME(MycatBuiltInMethodImpl.class, "bigintToDate", Long.class),
    DECIMAL_TO_TIME(MycatBuiltInMethodImpl.class, "decimalToDate", BigDecimal.class),
    FLOAT_TO_TIME(MycatBuiltInMethodImpl.class, "floatToDate", Double.class),
    REAL_TO_TIME(MycatBuiltInMethodImpl.class, "realToDate", Float.class),
    DOUBLE_TO_TIME(MycatBuiltInMethodImpl.class, "doubleToDate", Double.class),
    DATE_TO_TIME(MycatBuiltInMethodImpl.class, "dateToDate", LocalDate.class),
    TIME_TO_TIME(MycatBuiltInMethodImpl.class, "timeToDate", Duration.class),
    TIMESTAMP_TO_TIME(MycatBuiltInMethodImpl.class, "timestampToDate", LocalDateTime.class),
    PERIOD_TO_TIME(MycatBuiltInMethodImpl.class, "periodToDate", Period.class),
    BYTESTRING_TO_TIME(MycatBuiltInMethodImpl.class, "byteStringToDate", ByteString.class),

    BOOLEAN_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "booleanToTimestamp", Boolean.class),
    TINYINT_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "tinyintToTimestamp", Byte.class),
    SMALLINT_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "smallintToTimestamp", Short.class),
    INTEGER_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "integerToTimestamp", Long.class),
    BIGINT_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "bigintToTimestamp", Long.class),
    DECIMAL_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "decimalToTimestamp", BigDecimal.class),
    FLOAT_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "floatToTimestamp", Double.class),
    REAL_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "realToTimestamp", Float.class),
    DOUBLE_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "doubleToTimestamp", Double.class),
    DATE_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "dateToTimestamp", LocalDate.class),
    TIME_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "timeToTimestamp", Duration.class),
    TIMESTAMP_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "timestampToTimestamp", LocalDateTime.class),
    PERIOD_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "periodToTimestamp", Period.class),
    BYTESTRING_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "byteStringToTimestamp", ByteString.class),

    STRING_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "stringToBoolean", String.class),
    BOOLEAN_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "booleanToBoolean", Boolean.class),
    TINYINT_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "tinyintToBoolean", Byte.class),
    SMALLINT_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "smallintToBoolean", Short.class),
    INTEGER_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "integerToBoolean", Long.class),
    BIGINT_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "bigintToBoolean", Long.class),
    DECIMAL_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "decimalToBoolean", BigDecimal.class),
    FLOAT_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "floatToBoolean", Double.class),
    REAL_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "realToBoolean", Float.class),
    DOUBLE_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "doubleToBoolean", Double.class),
    DATE_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "dateToBoolean", LocalDate.class),
    TIME_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "timeToBoolean", Duration.class),
    PERIOD_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "periodToBoolean", Period.class),
    TIMESTAMP_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "timestampToBoolean", LocalDateTime.class),
    BYTESTRING_TO_BOOLEAN(MycatBuiltInMethodImpl.class, "byteStringToBoolean", ByteString.class),


    STRING_TO_PERIOD(MycatBuiltInMethodImpl.class, "stringToPeriod", String.class),
    BOOLEAN_TO_PERIOD(MycatBuiltInMethodImpl.class, "booleanToPeriod", Boolean.class),
    TINYINT_TO_PERIOD(MycatBuiltInMethodImpl.class, "tinyintToPeriod", Byte.class),
    SMALLINT_TO_PERIOD(MycatBuiltInMethodImpl.class, "smallintToPeriod", Short.class),
    INTEGER_TO_PERIOD(MycatBuiltInMethodImpl.class, "integerToPeriod", Long.class),
    BIGINT_TO_PERIOD(MycatBuiltInMethodImpl.class, "bigintToPeriod", Long.class),
    DECIMAL_TO_PERIOD(MycatBuiltInMethodImpl.class, "decimalToPeriod", BigDecimal.class),
    FLOAT_TO_PERIOD(MycatBuiltInMethodImpl.class, "floatToPeriod", Double.class),
    REAL_TO_PERIOD(MycatBuiltInMethodImpl.class, "realToPeriod", Float.class),
    DOUBLE_TO_PERIOD(MycatBuiltInMethodImpl.class, "doubleToPeriod", Double.class),
    DATE_TO_PERIOD(MycatBuiltInMethodImpl.class, "dateToPeriod", LocalDate.class),
    TIME_TO_PERIOD(MycatBuiltInMethodImpl.class, "timeToPeriod", Duration.class),
    TIMESTAMP_TO_PERIOD(MycatBuiltInMethodImpl.class, "timestampToPeriod", LocalDateTime.class),
    PERIOD_TO_PERIOD(MycatBuiltInMethodImpl.class, "periodToPeriod", Period.class),
    BYTESTRING_TO_PERIOD(MycatBuiltInMethodImpl.class, "byteStringToPeriod", ByteString.class),

    STRING_TO_DURATION(MycatBuiltInMethodImpl.class, "stringToDuration", String.class),
    BOOLEAN_TO_DURATION(MycatBuiltInMethodImpl.class, "booleanToDuration", Boolean.class),
    TINYINT_TO_DURATION(MycatBuiltInMethodImpl.class, "tinyintToDuration", Byte.class),
    SMALLINT_TO_DURATION(MycatBuiltInMethodImpl.class, "smallintToDuration", Short.class),
    INTEGER_TO_DURATION(MycatBuiltInMethodImpl.class, "integerToDuration", Long.class),
    BIGINT_TO_DURATION(MycatBuiltInMethodImpl.class, "bigintToDuration", Long.class),
    DECIMAL_TO_DURATION(MycatBuiltInMethodImpl.class, "decimalToDuration", BigDecimal.class),
    FLOAT_TO_DURATION(MycatBuiltInMethodImpl.class, "floatToDuration", Double.class),
    REAL_TO_DURATION(MycatBuiltInMethodImpl.class, "realToDuration", Float.class),
    DOUBLE_TO_DURATION(MycatBuiltInMethodImpl.class, "doubleToDuration", Double.class),
    DATE_TO_DURATION(MycatBuiltInMethodImpl.class, "dateToDuration", LocalDate.class),
    TIME_TO_DURATION(MycatBuiltInMethodImpl.class, "timeToDuration", Duration.class),
    TIMESTAMP_TO_DURATION(MycatBuiltInMethodImpl.class, "timestampToDuration", LocalDateTime.class),
    PERIOD_TO_DURATION(MycatBuiltInMethodImpl.class, "periodToDuration", Period.class),
    BYTESTRING_TO_DURATION(MycatBuiltInMethodImpl.class, "byteStringToDuration", ByteString.class),

    BOOLEAN_TO_STRING(MycatBuiltInMethodImpl.class, "stringToString", String.class),
    TINYINT_TO_STRING(MycatBuiltInMethodImpl.class, "booleanToString", Boolean.class),
    SMALLINT_TO_STRING(MycatBuiltInMethodImpl.class, "smallintToString", Short.class),
    INTEGER_TO_STRING(MycatBuiltInMethodImpl.class, "integerToString", Integer.class),
    BIGINT_TO_STRING(MycatBuiltInMethodImpl.class, "bigintToString", Long.class),
    DECIMAL_TO_STRING(MycatBuiltInMethodImpl.class, "decimalToString", BigDecimal.class),
    FLOAT_TO_STRING(MycatBuiltInMethodImpl.class, "floatToString", Double.class),
    REAL_TO_STRING(MycatBuiltInMethodImpl.class, "realToString", Float.class),
    DOUBLE_TO_STRING(MycatBuiltInMethodImpl.class, "doubleToString", Double.class),
    DATE_TO_STRING(MycatBuiltInMethodImpl.class, "dateToString", LocalDate.class),
    TIME_TO_STRING(MycatBuiltInMethodImpl.class, "timeToString", Duration.class),
    TIMESTAMP_TO_STRING(MycatBuiltInMethodImpl.class, "timestampToString", LocalDateTime.class),
    PERIOD_TO_STRING(MycatBuiltInMethodImpl.class, "periodToString", Period.class),
    BYTESTRING_TO_STRING(MycatBuiltInMethodImpl.class, "byteStringToString", ByteString.class),

    ;
    public final Method method;
    public final Constructor constructor;
    public final Field field;

    MycatBuiltInMethod(Method method, Constructor constructor, Field field) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }

    /** Defines a method. */
    MycatBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    /** Defines a constructor. */
    MycatBuiltInMethod(Class clazz, Class... argumentTypes) {
        this(null, Types.lookupConstructor(clazz, argumentTypes), null);
    }

    /** Defines a field. */
    MycatBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
        this(null, null, Types.lookupField(clazz, fieldName));
        assert dummy : "dummy value for method overloading must be true";
    }
}
