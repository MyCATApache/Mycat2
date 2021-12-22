package io.mycat.beans.mycat;

import com.google.flatbuffers.DoubleVector;
import com.google.protobuf.Int32Value;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.vertx.mysqlclient.impl.MySQLRowImpl;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.data.Numeric;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.SneakyThrows;
import org.apache.arrow.vector.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.List;
import java.util.function.Function;

import static java.sql.ResultSetMetaData.columnNoNulls;

public enum MycatDataType {
    BOOLEAN,
    BIT,
    TINYINT,
    UNSIGNED_TINYINT,
    SHORT,
    UNSIGNED_SHORT,
    INT,
    UNSIGNED_INT,
    LONG,
    UNSIGNED_LONG,
    DOUBLE,
    DECIMAL,
    DATE,
    DATETIME,
    TIME,
    YEAR,
    //    CHAR_BINARY,
    CHAR,
    //    VARCHAR_BINARY,
    VARCHAR,
    BINARY,
    FLOAT,
    NULL;

    public Class getJavaClass() {
        switch (this) {
            case BOOLEAN:
                return Boolean.TYPE;
            case BIT:
            case UNSIGNED_INT:
            case LONG:
                return Long.TYPE;
            case TINYINT:
                return Byte.TYPE;
            case UNSIGNED_TINYINT:
            case SHORT:
            case YEAR:
                return Short.TYPE;
            case UNSIGNED_SHORT:
            case INT:
                return Integer.TYPE;
            case UNSIGNED_LONG:
                return BigInteger.class;
            case DOUBLE:
                return Double.TYPE;
            case DECIMAL:
                return BigDecimal.class;
            case DATE:
                return LocalDate.class;
            case DATETIME:
                return LocalDateTime.class;
            case TIME:
                return Duration.class;
            case BINARY:
                return byte[].class;
            case CHAR:
            case VARCHAR:
                return String.class;
            case FLOAT:
                return Float.class;
            case NULL:
                return Void.class;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    public Class getPerformanceJavaClass() {
        switch (this) {
            case BOOLEAN:
                return Boolean.TYPE;
            case BIT:
            case UNSIGNED_INT:
            case LONG:
            case DATE:
            case DATETIME:
            case TIME:
            case UNSIGNED_LONG:
                return Long.TYPE;
            case TINYINT:
                return Byte.TYPE;
            case UNSIGNED_TINYINT:
            case SHORT:
            case YEAR:
                return Short.TYPE;
            case UNSIGNED_SHORT:
            case INT:
                return Integer.TYPE;
            case DOUBLE:
                return Double.TYPE;
            case DECIMAL:
                return BigDecimal.class;
            case BINARY:
            case CHAR:
            case VARCHAR:
                return byte[].class;
            case FLOAT:
                return Float.class;
            case NULL:
                return Void.class;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    @SneakyThrows
    public static MycatField[] from(ResultSetMetaData resultSetMetaData) {
        int columnCount = resultSetMetaData.getColumnCount();
        MycatField[] mycatFields = new MycatField[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int index = i + 1;
            String columnLabel = resultSetMetaData.getColumnLabel(index);
            String columnClassName = resultSetMetaData.getColumnTypeName(index).toLowerCase();
            boolean signed = resultSetMetaData.isSigned(index);
            boolean nullable = resultSetMetaData.isNullable(index) == columnNoNulls;
            int columnType = resultSetMetaData.getColumnType(index);
            int scale = resultSetMetaData.getScale(index);
            int precision = resultSetMetaData.getPrecision(index);
            mycatFields[i] = getMycatField(columnLabel, columnClassName, signed, nullable, columnType, scale, precision);
        }
        return mycatFields;
    }

    @NotNull
    public static MycatField getMycatField(String name, String columnClassName, boolean signed, boolean nullable, int columnType, int scale, int precision) {
        MycatDataType mycatDataType = null;
        switch (JDBCType.valueOf(columnType)) {
            case BIT:
                mycatDataType = BIT;
                break;
            case TINYINT:
                mycatDataType = signed ? TINYINT : UNSIGNED_TINYINT;
                break;
            case SMALLINT:
                mycatDataType = signed ? SHORT : UNSIGNED_SHORT;
                break;
            case INTEGER:
                mycatDataType = signed ? INT : UNSIGNED_INT;
                break;
            case BIGINT:
                mycatDataType = signed ? LONG : UNSIGNED_LONG;
                break;
            case FLOAT:
            case REAL:
                mycatDataType = MycatDataType.FLOAT;
                break;
            case DOUBLE:
                mycatDataType = MycatDataType.DOUBLE;
                break;
            case NUMERIC:
                String s = columnClassName.toLowerCase();
                if (s.contains("int")) {
                    if (!signed) {
                        mycatDataType = MycatDataType.UNSIGNED_LONG;
                    } else {
                        mycatDataType = MycatDataType.DECIMAL;
                    }
                } else {
                    mycatDataType = MycatDataType.DECIMAL;
                }
                break;
            case DECIMAL:
                mycatDataType = MycatDataType.DECIMAL;
                break;
            case CHAR:
                if (columnClassName.contains("char") || columnClassName.contains("str")) {
                    mycatDataType = CHAR;
                } else {
                    mycatDataType = BINARY;
                }
                break;
            case LONGVARCHAR:
            case VARCHAR:
                if (columnClassName.contains("char") || columnClassName.contains("str")) {
                    mycatDataType = VARCHAR;
                } else {
                    mycatDataType = BINARY;
                }
                break;
            case BOOLEAN:
                mycatDataType = BOOLEAN;
                break;
            case DATE:
                mycatDataType = DATE;
                break;
            case TIME:
            case TIME_WITH_TIMEZONE:
                mycatDataType = TIME;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                mycatDataType = DATETIME;
                break;
            case BLOB:
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                mycatDataType = BINARY;
                break;
            case NULL:
                mycatDataType = NULL;
                break;
            case CLOB:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
            case OTHER:
            case JAVA_OBJECT:
            case DISTINCT:
            case STRUCT:
            case ARRAY:
            case REF:
            case DATALINK:
            case ROWID:
            case SQLXML:
            case REF_CURSOR:
            default:
                mycatDataType = VARCHAR;
                break;
        }
        MycatField mycatField;
        if (mycatDataType == DECIMAL || mycatDataType == DOUBLE || mycatDataType == FLOAT) {
            mycatField = MycatField.of(name, mycatDataType, nullable, scale, precision);
        } else {
            mycatField = MycatField.of(name, mycatDataType, nullable);
        }
        return mycatField;
    }

    @SneakyThrows
    public static MycatField[] fromVertx(List<ColumnDescriptor> resultSetMetaData) {
        int columnCount = resultSetMetaData.size();
        MycatField[] mycatFields = new MycatField[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int index = i;
            ColumnDescriptor columnDefinition = resultSetMetaData.get(index);
            String columnClassName = columnDefinition.typeName().toLowerCase();
            boolean signed = !columnClassName.contains("unsigned");
            boolean nullable =
                    !(columnDefinition instanceof ColumnDefinition) ||
                            (((ColumnDefinition) columnDefinition).flags() & MySQLFieldsType.NOT_NULL_FLAG) == 0;
            int columnType = columnDefinition.jdbcType().getVendorTypeNumber();
            mycatFields[i] = getMycatField(columnDefinition.name(), columnClassName, signed, nullable, columnType, 0, 0);
        }
        return mycatFields;
    }

    @SneakyThrows
    public Object convert(ResultSet resultSet, MycatField[] mycatFields, int columnIndex) {
        MycatField mycatField = mycatFields[columnIndex];
        int jdbcResultColumnIndex = columnIndex + 1;
        return resultSet.getObject(jdbcResultColumnIndex, mycatField.getMycatDataType().getJavaClass());
    }

    @SneakyThrows
    public Object convert(Row resultSet, MycatField[] mycatFields, int columnIndex) {
        MycatField mycatField = mycatFields[columnIndex];
        return resultSet.get(mycatField.getMycatDataType().getJavaClass(), columnIndex);
    }

    @SneakyThrows
    public Object convert(Object[] resultSet, MycatField[] mycatFields, int columnIndex) {
        MycatField mycatField = mycatFields[columnIndex];
        Object o = resultSet[columnIndex];
        return convert(mycatField, o);
    }

    @Nullable
    public Object convert(MycatField mycatField, Object o) {
        if (mycatField.isNullable() && o == null) return null;
        return mycatField.getMycatDataType().fromValue(o);
    }

    Class<? extends FieldVector> getFieldVector() {
        switch (this) {
            case BOOLEAN:
                return BitVector.class;
            case BIT:
            case UNSIGNED_LONG:
                return UInt8Vector.class;
            case TINYINT:
                return TinyIntVector.class;
            case UNSIGNED_TINYINT:
                return UInt1Vector.class;
            case YEAR:
            case SHORT:
                return SmallIntVector.class;
            case UNSIGNED_SHORT:
                return UInt2Vector.class;
            case INT:
                return IntVector.class;
            case UNSIGNED_INT:
                return UInt4Vector.class;
            case DOUBLE:
                return Float8Vector.class;
            case LONG:
                return BigIntVector.class;
            case DECIMAL:
                return DecimalVector.class;
            case DATE:
                return DateMilliVector.class;
            case DATETIME:
                return TimeStampMilliVector.class;
            case TIME:
                return TimeMilliVector.class;
            case CHAR:
            case VARCHAR:
                return VarCharVector.class;
            case BINARY:
                return VarBinaryVector.class;
            case FLOAT:
                return Float4Vector.class;
            case NULL:
                return NullVector.class;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    @SneakyThrows
    public void convertToVector(ResultSet resultSet, int index, FieldVector fieldVector, int rowIndex) {
        switch (this) {
            case BOOLEAN: {
                boolean aBoolean = resultSet.getBoolean(index);
                BitVector bitVector = (BitVector) fieldVector;
                if (resultSet.wasNull()) {
                    bitVector.setNull(rowIndex);
                } else {
                    if (aBoolean) {
                        bitVector.setToOne(rowIndex);
                    } else {
                        bitVector.set(rowIndex, 0);
                    }
                }
                break;
            }

            case UNSIGNED_INT: {
                long aLong = resultSet.getLong(index);
                UInt4Vector uInt4Vector = (UInt4Vector) fieldVector;
                if (resultSet.wasNull()) {
                    uInt4Vector.setNull(rowIndex);
                } else {
                    uInt4Vector.setWithPossibleTruncate(rowIndex, aLong);
                }
                break;
            }

            case LONG: {
                long aLong = resultSet.getLong(index);
                BigIntVector uInt8Vector = (BigIntVector) fieldVector;
                if (resultSet.wasNull()) {
                    uInt8Vector.setNull(rowIndex);
                } else {
                    uInt8Vector.set(rowIndex, aLong);
                }
                break;
            }
            case TINYINT: {
                byte aByte = resultSet.getByte(index);
                TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
                if (resultSet.wasNull()) {
                    tinyIntVector.setNull(rowIndex);
                } else {
                    tinyIntVector.set(rowIndex, aByte);
                }
                break;
            }
            case UNSIGNED_TINYINT: {
                short aShort = resultSet.getShort(index);
                UInt1Vector tinyIntVector = (UInt1Vector) fieldVector;
                if (resultSet.wasNull()) {
                    tinyIntVector.setNull(rowIndex);
                } else {
                    tinyIntVector.set(rowIndex, aShort);
                }
                break;
            }
            case YEAR:
            case SHORT: {
                short aShort = resultSet.getShort(index);
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                if (resultSet.wasNull()) {
                    smallIntVector.setNull(rowIndex);
                } else {
                    smallIntVector.set(rowIndex, aShort);
                }
                break;
            }
            case UNSIGNED_SHORT: {
                int anInt = resultSet.getInt(index);
                UInt2Vector uInt2Vector = (UInt2Vector) fieldVector;
                if (resultSet.wasNull()) {
                    uInt2Vector.setNull(rowIndex);
                } else {
                    uInt2Vector.set(rowIndex, anInt);
                }
                break;
            }
            case INT: {
                int anInt = resultSet.getInt(index);
                IntVector intVector = (IntVector) fieldVector;
                if (resultSet.wasNull()) {
                    intVector.setNull(rowIndex);
                } else {
                    intVector.set(rowIndex, anInt);
                }
                break;
            }
            case BIT:
            case UNSIGNED_LONG: {
                BigInteger bigInteger = resultSet.getObject(index, BigInteger.class);
                UInt8Vector uInt8Vector = (UInt8Vector) fieldVector;
                if (resultSet.wasNull()) {
                    uInt8Vector.setNull(rowIndex);
                } else {
                    uInt8Vector.setWithPossibleTruncate(rowIndex, bigInteger.longValue());
                }
                break;
            }
            case DOUBLE: {
                double aDouble = resultSet.getDouble(index);
                Float8Vector doubleVector = (Float8Vector) fieldVector;
                if (resultSet.wasNull()) {
                    doubleVector.setNull(rowIndex);
                } else {
                    doubleVector.set(rowIndex, aDouble);
                }
                break;
            }
            case DECIMAL: {
                BigDecimal bigDecimal = resultSet.getBigDecimal(index);
                DecimalVector decimalVector = (DecimalVector) fieldVector;
                if (resultSet.wasNull()) {
                    decimalVector.setNull(rowIndex);
                } else {
                    decimalVector.set(rowIndex, bigDecimal);
                }
                break;
            }
            case DATE: {
                Date date = resultSet.getDate(index);
                DateMilliVector dateMilliVector = (DateMilliVector) fieldVector;
                if (resultSet.wasNull()) {
                    dateMilliVector.setNull(rowIndex);
                } else {
                    dateMilliVector.set(rowIndex, date.getTime());
                }
                break;
            }
            case DATETIME: {
                Timestamp timestamp = resultSet.getTimestamp(index);
                TimeStampMilliVector milliVector = (TimeStampMilliVector) fieldVector;
                if (resultSet.wasNull()) {
                    milliVector.setNull(rowIndex);
                } else {
                    milliVector.set(rowIndex, timestamp.getTime());
                }
                break;
            }
            case TIME: {
                Time time = resultSet.getTime(index);
                TimeMilliVector milliVector = (TimeMilliVector) fieldVector;
                if (resultSet.wasNull()) {
                    milliVector.setNull(rowIndex);
                } else {
                    milliVector.set(rowIndex, (int) time.getTime());
                }
                break;
            }
            default:
            case CHAR:
            case VARCHAR: {
                byte[] bytes = resultSet.getBytes(index);
                VarCharVector varCharVector = (VarCharVector) fieldVector;
                if (resultSet.wasNull()) {
                    varCharVector.setNull(rowIndex);
                } else {
                    varCharVector.set(rowIndex, bytes);
                }
                break;
            }
            case BINARY: {
                byte[] bytes = resultSet.getBytes(index);
                VarBinaryVector varCharVector = (VarBinaryVector) fieldVector;
                if (resultSet.wasNull()) {
                    varCharVector.setNull(rowIndex);
                } else {
                    varCharVector.set(rowIndex, bytes);
                }
                break;
            }
            case FLOAT: {
                float aFloat = resultSet.getFloat(index);
                Float4Vector doubleVector = (Float4Vector) fieldVector;
                if (resultSet.wasNull()) {
                    doubleVector.setNull(rowIndex);
                } else {
                    doubleVector.set(rowIndex, aFloat);
                }
                break;
            }
            case NULL:

        }
    }

    @SneakyThrows
    public Object convertToObject(ResultSet resultSet, int index) {
        switch (this) {
            case BOOLEAN:
                boolean aBoolean = resultSet.getBoolean(index);
                return resultSet.wasNull() ? null : aBoolean;
            case BIT:
            case UNSIGNED_INT:
            case LONG:
                long aBit = resultSet.getLong(index);
                return resultSet.wasNull() ? null : aBit;
            case TINYINT:
                short aByte = resultSet.getByte(index);
                return resultSet.wasNull() ? null : aByte;
            case UNSIGNED_TINYINT:
            case SHORT:
            case YEAR:
                int aShort = resultSet.getShort(index);
                return resultSet.wasNull() ? null : aShort;
            case UNSIGNED_SHORT:
            case INT:
                int anInt = resultSet.getInt(index);
                return resultSet.wasNull() ? null : anInt;
            case UNSIGNED_LONG:
                return resultSet.getObject(index, BigInteger.class);
            case DOUBLE:
                double aDouble = resultSet.getDouble(index);
                return resultSet.wasNull() ? null : aDouble;
            case DECIMAL:
                return resultSet.getBigDecimal(index);
            case DATE:
                Date date = resultSet.getDate(index);
                return date == null ? null : date.toLocalDate();
            case DATETIME:
                Timestamp timestamp = resultSet.getTimestamp(index);
                return timestamp == null ? null : timestamp.toLocalDateTime();
            case TIME:
                Time time = resultSet.getTime(index);
                return time == null ? null : time.toLocalTime();
            default:
            case CHAR:
            case VARCHAR:
                return resultSet.getString(index);
            case BINARY:
                return resultSet.getBytes(index);
            case FLOAT:
                float aFloat = resultSet.getFloat(index);
                return resultSet.wasNull() ? null : aFloat;
            case NULL:
                return null;
        }
    }

    @SneakyThrows
    public Object convertToObject(Row row, int index) {
        return fromValue(row.getValue(index));
    }

    @SneakyThrows
    public Object fromValue(Object o) {
        switch (this) {
            case BOOLEAN:
                if (o == Boolean.TRUE || o == Boolean.FALSE) return o;
                if (o instanceof Number) {
                    if (((Number) o).byteValue() > 0) {
                        return Boolean.TRUE;
                    } else {
                        return Boolean.FALSE;
                    }
                }
                if (o instanceof byte[]) {
                    o = new String((byte[]) o);
                }
                String s = (String) o;
                return s.startsWith("t") || s.startsWith("T");
            case BIT:
                if (o instanceof Boolean) {
                    return (Boolean) o ? 1L : 0L;
                }
                if (o instanceof Long) {
                    return (Long) o;
                }
                if (o instanceof Number) {
                    return ((Number) o).longValue();
                }
                if (o instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) o).getLong();
                }
                if (o instanceof BitSet) {
                    return ((BitSet) o).toLongArray()[0];
                }
                break;
            case TINYINT:
                if (o instanceof Boolean) {
                    return (Boolean) o ? 1L : 0L;
                }
                if (o instanceof Byte) {
                    return (Byte) o;
                }
                if (o instanceof Short) {
                    return (Short) o;
                }
                if (o instanceof Number) {
                    return ((Number) o).shortValue();
                }
                break;
            case UNSIGNED_TINYINT:
            case SHORT:
                if (o instanceof Short) {
                    return (Short) o;
                }
                if (o instanceof Number) {
                    return ((Number) o).shortValue();
                }
                break;
            case UNSIGNED_SHORT:
            case INT:
                if (o instanceof Integer) {
                    return (Integer) o;
                }
                if (o instanceof Number) {
                    return ((Number) o).intValue();
                }
                break;
            case UNSIGNED_INT:
            case LONG:
                if (o instanceof Long) {
                    return (Long) o;
                }
                if (o instanceof Number) {
                    return ((Number) o).longValue();
                }
                break;
            case UNSIGNED_LONG:
                if (o instanceof BigInteger) {
                    return (BigInteger) o;
                }
                if (o instanceof Long) {
                    return BigInteger.valueOf((Long) o);
                }
                if (o instanceof Numeric) {
                    return ((Numeric) o).bigIntegerValue();
                }
                if (o instanceof Number) {
                    return new BigInteger(((Number) o).toString());
                }

                break;
            case DOUBLE:
                if (o instanceof Double) {
                    return (Double) o;
                }
                if (o instanceof Number) {
                    return ((Number) o).doubleValue();
                }
                return new BigDecimal(o.toString()).doubleValue();
            case DECIMAL:
                if (o instanceof BigDecimal) {
                    return (BigDecimal) o;
                }
                if (o instanceof BigInteger) {
                    return new BigDecimal(o.toString());
                }
                if (o instanceof Numeric) {
                    return ((Numeric) o).bigDecimalValue();
                }
                if (o instanceof Number) {
                    return new BigDecimal(o.toString());
                }
                return new BigDecimal(o.toString());
            case DATE:
                if (o instanceof Date) {
                    return ((Date) o).toLocalDate();
                }
                if (o instanceof java.util.Date) {
                    long time = ((java.util.Date) o).getTime();
                    return (new Date(time)).toLocalDate();
                }
                if (o instanceof LocalDate) {
                    return o;
                }
                throw new UnsupportedOperationException();
            case DATETIME:
                if (o instanceof Timestamp) {
                    return ((Timestamp) o).toLocalDateTime();
                }
                if (o instanceof LocalDateTime) {
                    return o;
                }
                throw new UnsupportedOperationException();
            case TIME:
                if (o instanceof Time) {
                    Time time = (Time) o;
                    o = time.toLocalTime();
                }
                if (o instanceof LocalTime) {
                    LocalTime time = (LocalTime) o;
                    return toDuration(time);
                }
                if (o instanceof Duration) {
                    return o;
                }
                break;
            case YEAR:
                if (o instanceof Number) {
                    return ((Number) o).shortValue();
                }
                throw new UnsupportedOperationException();
            default:
            case VARCHAR:
            case CHAR:
                if (o instanceof String) {
                    return o;
                }
                if (o instanceof Character) {
                    return String.valueOf(((Character) o).charValue());
                }
                if (o instanceof Clob) {
                    Clob clob = (Clob) o;
                    return String.valueOf(clob.getSubString(1, (int) clob.length()));
                }
                throw new UnsupportedOperationException();
            case BINARY:
                if (o instanceof Blob) {
                    return ((Blob) o).getBytes(1, (int) ((Blob) o).length());
                }
                if (o instanceof byte[]) {
                    return ((byte[]) o);
                }
                if (o instanceof ByteBuffer) {
                    return ((ByteBuffer) o).array();
                }
                throw new UnsupportedOperationException();
            case FLOAT:
                if (o instanceof Float) {
                    return o;
                }
                if (o instanceof Double) {
                    return ((Double) o).floatValue();
                }
                if (o instanceof Number) {
                    return ((Number) o).floatValue();
                }
                throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException();
    }

    private Duration toDuration(LocalTime time) {
        return Duration.ofHours(time.getHour()).plusMinutes(time.getMinute()).plusSeconds(time.getSecond());
    }

    public JDBCType getSignedJdbcType() {
        switch (this) {
            case BOOLEAN:
                return JDBCType.BOOLEAN;
            case BIT:
                return JDBCType.BIT;
            case TINYINT:
                return JDBCType.TINYINT;
            case SHORT:
            case UNSIGNED_TINYINT:
                return JDBCType.SMALLINT;
            case UNSIGNED_SHORT:
            case INT:
                return JDBCType.INTEGER;
            case UNSIGNED_INT:
            case LONG:
                return JDBCType.BIGINT;
            case UNSIGNED_LONG:
                return JDBCType.NUMERIC;
            case DOUBLE:
                return JDBCType.DOUBLE;
            case DECIMAL:
                return JDBCType.DECIMAL;
            case DATE:
                return JDBCType.DATE;
            case DATETIME:
                return JDBCType.TIMESTAMP;
            case TIME:
                return JDBCType.TIME;
            case YEAR:
                return JDBCType.SMALLINT;
            case CHAR:
                return JDBCType.CHAR;
            case VARCHAR:
                return JDBCType.VARCHAR;
            case BINARY:
                return JDBCType.BINARY;
            case FLOAT:
                return JDBCType.FLOAT;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    public static MycatDataType fromJdbc(JDBCType jdbcType, boolean signed) {
        switch (jdbcType) {
            case BIT:
                return MycatDataType.BIT;
            case TINYINT:
                return signed ? MycatDataType.TINYINT : MycatDataType.UNSIGNED_TINYINT;
            case SMALLINT:
                return signed ? MycatDataType.SHORT : MycatDataType.UNSIGNED_SHORT;
            case INTEGER:
                return signed ? MycatDataType.INT : MycatDataType.UNSIGNED_INT;
            case BIGINT:
                return signed ? MycatDataType.LONG : MycatDataType.UNSIGNED_LONG;
            case REAL:
            case FLOAT:
                return MycatDataType.FLOAT;
            case DOUBLE:
                return MycatDataType.DOUBLE;
            case DECIMAL:
            case NUMERIC:
                return MycatDataType.DECIMAL;
            case CHAR:
                return MycatDataType.CHAR;
            case VARCHAR:
            case LONGVARCHAR:
                return MycatDataType.VARCHAR;
            case DATE:
                return MycatDataType.DATE;
            case TIME:
                return MycatDataType.TIME;
            case TIMESTAMP:
                return MycatDataType.DATETIME;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                return MycatDataType.BINARY;
            case NULL:
                return MycatDataType.NULL;
            case OTHER:
            case JAVA_OBJECT:
            case DISTINCT:
            case STRUCT:
            case ARRAY:
            case REF:
            case DATALINK:
                return MycatDataType.VARCHAR;
            case BLOB:
                return MycatDataType.BINARY;
            case CLOB:
                return MycatDataType.VARCHAR;
            case BOOLEAN:
                return MycatDataType.BOOLEAN;
            case ROWID:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
            case SQLXML:
            case REF_CURSOR:
                return MycatDataType.VARCHAR;
            case TIME_WITH_TIMEZONE:
                return MycatDataType.TIME;
            case TIMESTAMP_WITH_TIMEZONE:
                return MycatDataType.DATETIME;
            default:
                return MycatDataType.VARCHAR;
        }
    }

}
