package io.mycat;

import io.mycat.router.CustomRuleFunction;
import lombok.*;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/

@Getter
@EqualsAndHashCode
@ToString
public class SimpleColumnInfo {
    @NonNull
    final String columnName;
    final int precision;
    final int scale;
    @NonNull
    final JDBCType jdbcType;
    final boolean nullable;
    final boolean autoIncrement;
    final boolean primaryKey;
    final boolean index;
    /**
     * 在当前表中的下标 table(name,pwd,phone) 对应ID (0,1,2)
     */
    final int id;


    public SimpleColumnInfo(@NonNull String columnName, int precision, int scale, @NonNull JDBCType jdbcType, boolean nullable, boolean autoIncrement, boolean primaryKey, boolean index,int id) {
        this.columnName = columnName;
        this.precision = precision;
        this.scale = scale;
        this.jdbcType = jdbcType;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
        this.primaryKey = primaryKey;
        this.index = index || primaryKey;
        this.id = id;
    }

    public Type getType() {
        switch (jdbcType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case NUMERIC:
            case DECIMAL:
                return Type.NUMBER;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NULL:
            case OTHER:
            case JAVA_OBJECT:
            case DISTINCT:
            case STRUCT:
            case ARRAY:
            case REF:
            case DATALINK:
            case BOOLEAN:
            case ROWID:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
            case SQLXML:
            case REF_CURSOR:
                return Type.STRING;
            case DATE:
                return Type.DATE;
            case TIME:
                return Type.TIME;
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                return Type.TIMESTAMP;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
            case CLOB:
                return Type.BLOB;
            default:
                throw new IllegalStateException("Unexpected value: " + jdbcType);
        }

    }

    public Object normalizeValue(Object o) {
        switch (getType()) {
            case NUMBER:
                if (o instanceof String) {
                    return new BigDecimal((String) o);
                }
                if (o instanceof Number) {
                    return o;
                }
                throw new IllegalArgumentException();
            case STRING:
                return Objects.toString(o);
            case BLOB:
                break;
            case TIME:
                if (o instanceof String) {
                    return MycatTimeUtil.timeStringToTimeDuration((String) o);
                }
                if (o instanceof Duration) {
                    return o;
                }
                throw new IllegalArgumentException();
            case DATE:
                if (o instanceof String) {
                    Temporal temporal = MycatTimeUtil.timestampStringToTimestamp((String) o);
                    if (temporal instanceof LocalDateTime) {
                        return ((LocalDateTime) temporal).toLocalDate();
                    }
                    if (temporal instanceof LocalDate) {
                        return ((LocalDate) temporal);
                    }
                    throw new IllegalArgumentException();
                }
                if (o instanceof LocalDateTime) {
                    return o;
                }
                if (o instanceof LocalDate) {
                    return ((LocalDate) o).atStartOfDay();
                }
                throw new IllegalArgumentException();
            case TIMESTAMP:
                if (o instanceof String) {
                    return MycatTimeUtil.timestampStringToTimestamp((String) o);
                }
                if (o instanceof LocalDateTime) {
                    return o;
                }
                if (o instanceof LocalDate) {
                    return ((LocalDate) o).atStartOfDay();
                }
                throw new IllegalArgumentException();
        }
        throw new IllegalArgumentException();
    }

    /**
     * jamie 2019-12-11
     */
    public enum ShardingType {
        MAP_TARGET,
        MAP_SCHEMA,
        MAP_TABLE,
        NATURE_DATABASE_TABLE;

        public static ShardingType parse(String name) {
            if (name == null) {
                return NATURE_DATABASE_TABLE;
            }
            return valueOf(name);
        }
    }

    public enum Type {
        NUMBER,
        STRING,
        BLOB,
        TIME,
        DATE,
        TIMESTAMP
    }

    /**
     * jamie 2019-12-11
     */
    @Data
    @AllArgsConstructor
    @ToString
    public static class ShardingInfo {
        @NonNull
        final SimpleColumnInfo columnInfo;
        @NonNull
        final ShardingType shardingType;
        @NonNull
        final List<String> map;
        @NonNull
        final CustomRuleFunction function;
    }
}