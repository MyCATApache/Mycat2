package io.mycat.calcite.metadata;

import io.mycat.router.RuleFunction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/

@Getter
@AllArgsConstructor
public class SimpleColumnInfo {
    @NonNull
    final String columnName;
    final int dataType;
    final int precision;
    final int scale;
    @NonNull
    final JDBCType jdbcType;
    final boolean nullable;
    @NonNull
    final List<ShardingInfo> shardingInfo = new ArrayList<>();

    /**
     * jamie 2019-12-11
     */
    public enum ShardingType {
        MAP_TARGET,
        MAP_DATABASE,
        MAP_TABLE,
        NATURE_DATABASE_TABLE,

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
        final List<String> map ;
        @NonNull
        final RuleFunction function;
    }
}