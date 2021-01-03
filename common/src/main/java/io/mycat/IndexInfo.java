package io.mycat;

import lombok.Getter;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * 索引定义
 * @author wangzihaogithub 2020年12月19日22:43:53
 */
@Getter
public class IndexInfo {
    private final String schemaName;
    private final String tableName;
    private final String indexName;
    /**
     * 分区
     */
    private final DBPartitionBy dbPartitionBy;
    /**
     * 索引列(保留了字段的下标映射, 数组下标就是字段下标, 方便查询. 非索引字段值为NULL)
     */
    private final SimpleColumnInfo[] mapIndexes;
    private final SimpleColumnInfo[] indexes;
    /**
     * 覆盖列(保留了字段的下标映射, 数组下标就是字段下标, 方便查询. 非索引字段值为NULL)
     */
    private final SimpleColumnInfo[] mapCovering;
    private final SimpleColumnInfo[] covering;

    public IndexInfo(String schemaName,String tableName,String indexName, SimpleColumnInfo[] mapIndexes,SimpleColumnInfo[] mapCovering, DBPartitionBy dbPartitionBy) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
        this.mapIndexes = mapIndexes;
        this.mapCovering = mapCovering;
        this.dbPartitionBy = dbPartitionBy;
        this.indexes = Stream.of(mapIndexes).filter(Objects::nonNull).toArray(SimpleColumnInfo[]::new);
        this.covering = Stream.of(mapCovering).filter(Objects::nonNull).toArray(SimpleColumnInfo[]::new);
        for (SimpleColumnInfo index : indexes) {
            index.getIndexCoveringList().add(this);
        }
        for (SimpleColumnInfo columnInfo : covering) {
            columnInfo.getIndexCoveringList().add(this);
        }
    }

    @Getter
    public static class DBPartitionBy{
        private final String methodName;
        /**
         * 分区列(保留了字段的下标映射, 数组下标就是字段下标, 方便查询. 非索引字段值为NULL)
         */
        private final SimpleColumnInfo[] mapColumns;
        private final SimpleColumnInfo[] columns;

        public DBPartitionBy(String methodName, SimpleColumnInfo[] mapColumns) {
            this.methodName = methodName;
            this.mapColumns = mapColumns;
            this.columns = Stream.of(mapColumns).filter(Objects::nonNull).toArray(SimpleColumnInfo[]::new);
        }
    }

    @Override
    public String toString() {
        return schemaName+"."+tableName+"."+indexName;
    }
}
