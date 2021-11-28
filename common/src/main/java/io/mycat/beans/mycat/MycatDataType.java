package io.mycat.beans.mycat;

import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.data.Numeric;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

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
    CHAR_BINARY,
    CHAR,
    VARCHAR_BINARY,
    VARCHAR,
    BINARY,
    FLOAT;

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
            case CHAR_BINARY:
            case BINARY:
            case VARCHAR_BINARY:
                return byte[].class;
            case CHAR:
            case VARCHAR:
                return String.class;
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
            case CHAR_BINARY:
            case BINARY:
            case VARCHAR_BINARY:
            case CHAR:
            case VARCHAR:
                return byte[].class;
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
    private static MycatField getMycatField(String name, String columnClassName, boolean signed, boolean nullable, int columnType, int scale, int precision) {
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
                    mycatDataType = CHAR_BINARY;
                }
                break;
            case LONGVARCHAR:
            case VARCHAR:
                if (columnClassName.contains("char") || columnClassName.contains("str")) {
                    mycatDataType = VARCHAR;
                } else {
                    mycatDataType = VARCHAR_BINARY;
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
                break;
            default:
                mycatDataType = VARCHAR;
                break;
        }
        MycatField mycatField;
        if (mycatDataType == DECIMAL) {
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
        if (mycatField.isNullable() && o == null) return null;
        return mycatField.getMycatDataType().fromValue(o);
    }

    @SneakyThrows
    private Object fromValue(Object o) {
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
                    return BigInteger.valueOf(((Number) o).longValue());
                }

                break;
            case DOUBLE:
                if (o instanceof Double) {
                    return (Double) o;
                }
                if (o instanceof Number) {
                    return ((Number) o).doubleValue();
                }
                break;
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
                break;
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
                break;
            case DATETIME:
                if (o instanceof Timestamp) {
                    return ((Timestamp) o).toLocalDateTime();
                }
                if (o instanceof LocalDateTime) {
                    return o;
                }
                break;
            case TIME:
                if (o instanceof Time) {
                    Time time = (Time) o;
                    o = time.toLocalTime();
                }
                if (o instanceof LocalTime) {
                    LocalTime time = (LocalTime) o;
                    return Duration.ofHours(time.getHour()).plusMinutes(time.getMinute()).plusSeconds(time.getSecond());
                }
                break;
            case YEAR:
                if (o instanceof Number) {
                    return ((Number) o).shortValue();
                }
                break;
            case CHAR_BINARY:
            case VARCHAR_BINARY:
                if (o instanceof String) {
                    return ((String) o).getBytes(StandardCharsets.UTF_8);
                }
                if (o instanceof byte[]) {
                    return o;
                }
                break;
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
                    return String.valueOf(clob.getSubString(1,(int)clob.length()));
                }
                break;

            case BINARY:
                if (o instanceof Blob) {
                    return ((Blob) o).getBytes(1, (int)((Blob) o).length());
                }
                if (o instanceof byte[]) {
                    return ((byte[]) o);
                }
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
                break;
        }
        throw new UnsupportedOperationException();
    }
}
