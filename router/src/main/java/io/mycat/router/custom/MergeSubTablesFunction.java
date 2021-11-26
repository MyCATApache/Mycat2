/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router.custom;

import com.alibaba.druid.sql.SQLUtils;
import com.google.common.collect.ImmutableList;
import io.mycat.Partition;
import io.mycat.RangeVariable;
import io.mycat.ShardingTableType;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author chenjunwen
 */
public class MergeSubTablesFunction extends CustomRuleFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeSubTablesFunction.class);
    Partition defaultPartition;
    String tablePrefix;
    int beginIndex;
    int endIndex;
    private boolean segmentQuery;
    String columnName;

    @Override
    public String name() {
        return "MergeSubTables";
    }

    @Override
    public List<Partition> calculate(Map<String,RangeVariable> values) {
        ArrayList<Partition> res = new ArrayList<>();
        for (RangeVariable rangeVariable : values.values()) {
            //匹配字段名
            if (getColumnName().equalsIgnoreCase(rangeVariable.getColumnName())) {
                ///////////////////////////////////////////////////////////////
                String begin = Objects.toString(rangeVariable.getBegin());
                String end = Objects.toString(rangeVariable.getEnd());
                switch (rangeVariable.getOperator()) {
                    case EQUAL: {
                        Partition partition = this.calculate(begin);
                        if (partition != null) {
                            CollectionUtil.setOpAdd(res, partition);
                        } else {
                            return getTable().dataNodes();
                        }
                        break;
                    }
                    case RANGE: {
                        List<Partition> partitions = this.calculateRange(begin, end);
                        if (partitions == null || partitions.size() == 0) {
                            return getTable().dataNodes();
                        }
                        CollectionUtil.setOpAdd(res, partitions);
                        break;
                    }
                }
            }
        }
        return res.isEmpty() ? getTable().dataNodes() : res;
    }

    private String getColumnName() {
        return columnName;
    }


    public Partition calculate(String columnValue) {
        if (columnValue == null) {
            return defaultPartition;
        }
        String tableName = tablePrefix + columnValue.substring(beginIndex, endIndex);
        return getDataNode(tableName);
    }


    public List<Partition> calculateRange(String beginValue, String endValue) {
        if (segmentQuery) {
            if (beginValue == null) {
                return ImmutableList.of(defaultPartition);
            }
            if (endValue == null) {
                return ImmutableList.of(defaultPartition);
            }
            int begin = Integer.parseInt(beginValue.substring(beginIndex, endIndex));
            int end = Integer.parseInt(endValue.substring(beginIndex, endIndex));
            ArrayList<Partition> res = new ArrayList<>();
            for (int suffix = begin; suffix <= end; suffix++) {
                String suffixName = tablePrefix + suffix;
                res.add(getDataNode(suffixName));
            }
            return res;
        } else {
            return ImmutableList.of(defaultPartition);
        }
    }

    @Override
    protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {

        LOGGER.debug("properties:{}", properties);
        LOGGER.debug("ranges:{}", ranges);

        this.tablePrefix = Objects.toString(Objects.requireNonNull(properties.get("tablePrefix"), "tablePrefix required "));
        this.beginIndex = Integer.parseInt(Objects.toString(Objects.requireNonNull(properties.get("beginIndex"), "beginIndex required ")));
        this.endIndex = Integer.parseInt(Objects.toString(Objects.requireNonNull(properties.get("endIndex"), "endIndex required ")));
        this.columnName = Objects.toString(properties.get("columnName"));
        String targetName = Objects.toString(Objects.requireNonNull(properties.get("targetName")));
        String schema = Objects.toString(Objects.requireNonNull(properties.get("schemaName")));
        String table = Objects.toString(Objects.requireNonNull(properties.get("tableName")));

        this.segmentQuery = Boolean.parseBoolean(Objects.toString(properties.getOrDefault("segmentQuery", Boolean.FALSE.toString())));

        this.defaultPartition = new Partition() {
            @Override
            public String getTargetName() {
                return targetName;
            }

            @Override
            public String getSchema() {
                return schema;
            }

            @Override
            public String getTable() {
                return table;
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
    }

    @Override
    public boolean isShardingKey(String name) {
        return isShardingTableKey(name);
    }

    @Override
    public boolean isShardingDbKey(String name) {
        return false;
    }

    @Override
    public boolean isShardingTableKey(String name) {
        return columnName.equalsIgnoreCase(SQLUtils.normalize(name));
    }

    @Override
    public boolean isShardingTargetKey(String name) {
        return isShardingTableKey(name);
    }

    @Override
    public String getErUniqueID() {
        return  getClass().getName()+":"+defaultPartition + tablePrefix + beginIndex + endIndex + segmentQuery;
    }

    @Override
    public ShardingTableType getShardingTableType() {
        return ShardingTableType.SINGLE_INSTANCE_SHARDING_TABLE;
    }

    private Partition getDataNode(String tableName) {
        return new Partition() {
            @Override
            public String getTargetName() {
                return defaultPartition.getTargetName();
            }

            @Override
            public String getSchema() {
                return defaultPartition.getSchema();
            }

            @Override
            public String getTable() {
                return tableName;
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
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (MergeSubTablesFunction.class.isAssignableFrom(customRuleFunction.getClass())) {
            MergeSubTablesFunction tablesFunction = (MergeSubTablesFunction) customRuleFunction;
            Partition defaultPartition = tablesFunction.defaultPartition;
            String tablePrefix = tablesFunction.tablePrefix;
            int beginIndex = tablesFunction.beginIndex;
            int endIndex = tablesFunction.endIndex;
            boolean segmentQuery = tablesFunction.segmentQuery;
            return Objects.equals(this.defaultPartition, defaultPartition) &&
                    Objects.equals(this.tablePrefix, tablePrefix) &&
                    Objects.equals(this.beginIndex, beginIndex) &&
                    Objects.equals(this.endIndex, endIndex) &&
                    Objects.equals(this.segmentQuery, segmentQuery);
        }

        return super.isSameDistribution(customRuleFunction);
    }

    @Override
    public int requireShardingKeyCount() {
        return 1;
    }

    @Override
    public boolean requireShardingKeys(Set<String> shardingKeys) {
        return shardingKeys.contains(columnName);
    }
}