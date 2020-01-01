package io.mycat.calcite;

import io.mycat.router.RuleFunction;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/

@Getter
@AllArgsConstructor
class SimpleColumnInfo {
    @NonNull
    final String columnName;
    final int dataType;
    final int precision;
    final int scale;
    @NonNull
    final String typeString;
    final boolean nullable;
    @NonNull
    final List<ShardingInfo> shardingInfo = new ArrayList<>();

    public enum ShardingType {
        MAP_CLUSTER,
        MAP_TABLE,
        MAP_DATABASE,
        NATURE_DATABASE_TABLE,

    }


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