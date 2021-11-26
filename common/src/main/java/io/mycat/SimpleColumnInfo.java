/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.router.CustomRuleFunction;
import lombok.*;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
@EqualsAndHashCode
@Getter
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
    final boolean uniqueKey;
    /**
     * 是否是分片键
     */
    @Setter
    private boolean shardingKey;
    /**
     * 索引列
     */
    private final List<IndexInfo> indexKeyList = new ArrayList<>();
    /**
     * 覆盖列
     */
    private final List<IndexInfo> indexCoveringList = new ArrayList<>();
    /**
     * 在当前表中的下标 table(name,pwd,phone) 对应ID (0,1,2)
     */
    final int id;


    public SimpleColumnInfo(@NonNull String columnName, int precision, int scale, @NonNull JDBCType jdbcType, boolean nullable, boolean autoIncrement, boolean primaryKey, boolean uniqueKey, int id) {
        this.columnName = columnName;
        this.precision = precision;
        this.scale = scale;

        switch (jdbcType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                jdbcType = JDBCType.BIGINT;
                break;
            case NUMERIC:
                jdbcType = JDBCType.DECIMAL;
                break;
            case FLOAT:
            case REAL:
            case DOUBLE:
                jdbcType = JDBCType.DOUBLE;
                break;
            default:
                break;
        }
        this.jdbcType = jdbcType;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
        this.primaryKey = primaryKey;
        this.uniqueKey = uniqueKey;
        this.id = id;
    }

    /**
     * 是否是索引列
     *
     * @return true =是索引列
     */
    public boolean isUniqueKey() {
        for (IndexInfo indexInfo : indexKeyList) {
            for (SimpleColumnInfo indexInfoIndex : indexInfo.getIndexes()) {
                if (indexInfoIndex == this) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 是否是覆盖列
     *
     * @return true =是覆盖列
     */
    public boolean isIndexCovering() {
        for (IndexInfo indexInfo : indexCoveringList) {
            for (SimpleColumnInfo indexInfoIndex : indexInfo.getCovering()) {
                if (indexInfoIndex == this) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 是否是主键
     *
     * @return
     */
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    /**
     * 是否是分片键
     *
     * @return
     */
    public boolean isShardingKey() {
        return shardingKey;
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
        if (o == null) {
            return o;
        }
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
                    Temporal toTimestamp = MycatTimeUtil.timestampStringToTimestamp((String) o);
                    return toTimestamp;
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

    public boolean isUnique(){
        return primaryKey||uniqueKey;
    }
}