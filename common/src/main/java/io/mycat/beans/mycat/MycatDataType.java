package io.mycat.beans.mycat;

import io.mycat.beans.mysql.MySQLFieldsType;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
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

    public Class getMysqlColumnPacket() {
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
            mycatFields[i] = getMycatField(columnLabel,columnClassName, signed, nullable, columnType, scale, precision);
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
        if (mycatDataType == DECIMAL){
            mycatField = MycatField.of(name,mycatDataType, nullable, scale, precision);
        }else {
            mycatField=MycatField.of(name,mycatDataType, nullable);
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
            String columnClassName =columnDefinition.typeName().toLowerCase();
            boolean signed = !columnClassName.contains("unsigned");
            boolean nullable =
                    !(columnDefinition instanceof ColumnDefinition) ||
                            (((ColumnDefinition) columnDefinition).flags() & MySQLFieldsType.NOT_NULL_FLAG) == 0;
            int columnType = columnDefinition.jdbcType().getVendorTypeNumber();
            mycatFields[i] = getMycatField(columnDefinition.name(),columnClassName, signed, nullable, columnType, 0, 0);
        }
        return mycatFields;
    }
}
