package io.mycat.router.mycat1xfunction;

import io.mycat.*;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class TableHandlerMocks {

    public static ShardingTableHandler mockTableHandlerWithDataNodes(int count) {
        ArrayList<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String name = String.valueOf(i);
            Partition partition = new Partition() {

                @Override
                public String getTargetName() {
                    return "targetName";
                }

                @Override
                public String getSchema() {
                    return "schema";
                }

                @Override
                public String getTable() {
                    return name;
                }

                @Override
                public Integer getDbIndex() {
                    return null;
                }

                @Override
                public Integer getTableIndex() {
                    return null;
                }

                @Override
                public Integer getIndex() {
                    return null;
                }
            };
            partitions.add(partition);
        }
        return new ShardingTableHandler() {

            @Override
            public CustomRuleFunction function() {
               throw new UnsupportedOperationException();
            }

            @Override
            public List<Partition> dataNodes() {
                return partitions;
            }

            @Override
            public ShardingTableType shardingType() {
                return ShardingTableType.compute(dataNodes());
            }

            @Override
            public List<SimpleColumnInfo> getColumns() {
                return null;
            }

            @Override
            public Map<String,IndexInfo> getIndexes() {
                return null;
            }

            @Override
            public Optional canIndexTableScan(int[] projects) {
                return Optional.empty();
            }

            @Override
            public Optional<Iterable<Object[]>> canIndexTableScan(int[] projects, int[] filterIndex, Object[] value) {
                return Optional.empty();
            }


            @Override
            public Optional canIndexTableScan() {
                return Optional.empty();
            }

            @Override
            public boolean canIndex() {
                return false;
            }

            @Override
            public int getIndexBColumnName(String name) {
                return 0;
            }


            @Override
            public LogicTableType getType() {
                return null;
            }

            @Override
            public String getSchemaName() {
                return null;
            }

            @Override
            public String getTableName() {
                return null;
            }

            @Override
            public String getCreateTableSQL() {
                return null;
            }

            @Override
            public SimpleColumnInfo getColumnByName(String name) {
                return null;
            }

            @Override
            public SimpleColumnInfo getAutoIncrementColumn() {
                return null;
            }

            @Override
            public String getUniqueName() {
                return null;
            }

            @Override
            public Supplier<Number> nextSequence() {
                return null;
            }

            @Override
            public void createPhysicalTables() {

            }

            @Override
            public void dropPhysicalTables() {

            }
        };
    }
}