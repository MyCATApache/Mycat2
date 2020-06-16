package io.mycat.router.function;

import io.mycat.*;
import io.mycat.router.ShardingTableHandler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class TableHandlerMocks {

    public static ShardingTableHandler mockTableHandlerWithDataNodes(int count) {
        ArrayList<DataNode> dataNodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String name = String.valueOf(i);
            DataNode dataNode = new DataNode() {

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
            };
            dataNodes.add(dataNode);
        }
        return new ShardingTableHandler() {
            @Override
            public boolean isNatureTable() {
                return false;
            }

            @Override
            public List<DataNode> getShardingBackends() {
                return dataNodes;
            }

            @Override
            public SimpleColumnInfo.ShardingInfo getNatureTableColumnInfo() {
                return null;
            }

            @Override
            public SimpleColumnInfo.ShardingInfo getReplicaColumnInfo() {
                return null;
            }

            @Override
            public SimpleColumnInfo.ShardingInfo getDatabaseColumnInfo() {
                return null;
            }

            @Override
            public SimpleColumnInfo.ShardingInfo getTableColumnInfo() {
                return null;
            }

            @Override
            public List<SimpleColumnInfo> getColumns() {
                return null;
            }

            @Override
            public Function<ParseContext, Iterator<TextUpdateInfo>> insertHandler() {
                return null;
            }

            @Override
            public Function<ParseContext, Iterator<TextUpdateInfo>> updateHandler() {
                return null;
            }

            @Override
            public Function<ParseContext, Iterator<TextUpdateInfo>> deleteHandler() {
                return null;
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
            public Supplier<String> nextSequence() {
                return null;
            }
        };
    }
}