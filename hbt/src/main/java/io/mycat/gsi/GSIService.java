package io.mycat.gsi;

import io.mycat.IndexInfo;
import io.mycat.SimpleColumnInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * 全局二级索引 (Global Secondary Index, GSI)
 * <p>
 * create global index index_name on table_name(name...)
 * ---------------------------------------------------------------
 * | 数据源 | 主键列 | 索引列(支持多个) |  状态                       |
 * --------------------------------------------------------------|
 * |       | id    | name          | unCommit,preCommit,commit   |
 * --------------------------------------------------------------|
 * | db1   |  1    | 小王          |                              |
 * | db2   |  2    | 小李          |                              |
 * ---------------------------------------------------------------
 * <p>
 * 关于全局索引与数据库的一致性.
 * -----------------------------------------------------------
 * |角色|    操作                                              |
 * -----------------------------------------------------------
 * |gsi | insert     |        |  preCommit |        |  commit |
 * -----------------------------------------------------------
 * |db  | 关闭自动提交 | insert |            | commit |         |
 * -----------------------------------------------------------
 * <p>
 * 1. GSI启动时, 会检查状态
 * 如果状态小于 preCommit, 直接回滚.
 * 如果状态等于 preCommit, 则到db中同步当前行数据.
 * <p>
 * -----------------------------------------------
 * <p>
 * 存储接口 io.mycat.metadata.CustomTableHandler
 * mapdb
 * Chronicle-Map
 * apache ignite
 *
 * @author wangzihaogithub 2020年11月8日17:53:57
 */
public interface GSIService {

    Optional<Iterable<Object[]>> scanProject(String schemaName, String tableName, int[] projects);

    Optional<Iterable<Object[]>> scan(String schemaName, String tableName);

    Optional<Iterable<Object[]>> scanProjectFilter(String schemaName, String tableName,int index, Object value);

    Optional<Iterable<Object[]>> scanProjectFilter(String schemaName, String tableName, int[] projects, int[] filterIndexes, Object[] values);

    /**
     *
     * @param schemaName
     * @param tableName
     * @param index
     * @param value
     * @return 返回NULL=没有走索引, 返回空集合=不存在任何节点, 返回有数据=存在于集合中的节点
     */
    Collection<String> queryDataNode(String schemaName, String tableName, int index, Object value);

    boolean isIndexTable(String schemaName, String tableName);

    void insert(String txId, String schemaName, String tableName, SimpleColumnInfo[] columns, List<Object> objects,String dataNodeKey);

    boolean preCommit(String txId);

    boolean commit(String txId);

    boolean rollback(String txId);

    @Data
    class Transaction {
        private String id;
    }

    @Getter
    @AllArgsConstructor
    class RowIndexValues{
        private IndexInfo indexInfo;
        private final List<IndexValue> indexes = new ArrayList<>();
        private final List<IndexValue> coverings = new ArrayList<>();
        private final List<String> dataNodeKeyList = new ArrayList<>();

        @Override
        public String toString() {
            return indexInfo+", dataNode="+dataNodeKeyList+", indexes"+indexes+", coverings="+coverings;
        }
    }

    @Getter
    @AllArgsConstructor
    class IndexValue{
        private final SimpleColumnInfo column;
        private final Object value;

        @Override
        public String toString() {
            return column.getColumnName()+"="+value;
        }
    }
}
