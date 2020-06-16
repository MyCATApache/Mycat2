package io.mycat;

import io.mycat.router.CustomRuleFunction;
import lombok.*;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/

@Getter
@EqualsAndHashCode
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
    @NonNull
    final List<ShardingInfo> shardingInfo = new ArrayList<>();

    public SimpleColumnInfo(@NonNull String columnName, int precision, int scale, @NonNull JDBCType jdbcType, boolean nullable, boolean autoIncrement, boolean primaryKey, boolean index) {
        this.columnName = columnName;
        this.precision = precision;
        this.scale = scale;
        this.jdbcType = jdbcType;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
        this.primaryKey = primaryKey;
        this.index = index;
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

    /**
     * jamie 2019-12-11
     */
    @Data
    @AllArgsConstructor
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