package io.mycat.router.custom;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.RangeVariable;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


/**
 *@author  chenjunwen
 */
public class MergeSubTablesFunction extends CustomRuleFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeSubTablesFunction.class);
    DataNode defaultDataNode;
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
    public List<DataNode> calculate(Map<String, Collection<RangeVariable>> values) {
        ArrayList<DataNode> res = new ArrayList<>();
        for (RangeVariable rangeVariable : values.values().stream().flatMap(i->i.stream()).collect(Collectors.toList())) {
            //匹配字段名
            if (getColumnName().equalsIgnoreCase(rangeVariable.getColumnName())) {
                ///////////////////////////////////////////////////////////////
                String begin = Objects.toString(rangeVariable.getBegin());
                String end = Objects.toString(rangeVariable.getEnd());
                switch (rangeVariable.getOperator()) {
                    case EQUAL: {
                        DataNode dataNode = this.calculate(begin);
                        if (dataNode != null) {
                            CollectionUtil.setOpAdd(res, dataNode);
                        } else {
                            return getTable().dataNodes();
                        }
                        break;
                    }
                    case RANGE: {
                        List<DataNode> dataNodes = this.calculateRange(begin, end);
                        if (dataNodes == null || dataNodes.size() == 0) {
                            return getTable().dataNodes();
                        }
                        CollectionUtil.setOpAdd(res, dataNodes);
                        break;
                    }
                }
            }
        }
        return res.isEmpty()? getTable().dataNodes():res;
    }

    private String getColumnName() {
        return columnName;
    }


    public DataNode calculate(String columnValue) {
        if (columnValue == null) {
            return defaultDataNode;
        }
        String tableName = tablePrefix + columnValue.substring(beginIndex, endIndex);
        return getDataNode(tableName);
    }



    public List<DataNode> calculateRange(String beginValue, String endValue) {
        if (segmentQuery) {
            if (beginValue == null) {
                return ImmutableList.of(defaultDataNode);
            }
            if (endValue == null) {
                return ImmutableList.of(defaultDataNode);
            }
            int begin = Integer.parseInt(beginValue.substring(beginIndex, endIndex));
            int end = Integer.parseInt(endValue.substring(beginIndex, endIndex));
            ArrayList<DataNode> res = new ArrayList<>();
            for (int suffix = begin; suffix <= end; suffix++) {
                String suffixName = tablePrefix + suffix;
                res.add(getDataNode(suffixName));
            }
            return res;
        } else {
            return ImmutableList.of(defaultDataNode);
        }
    }

    @Override
    protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {

        LOGGER.debug("properties:{}", properties);
        LOGGER.debug("ranges:{}", ranges);

        this.tablePrefix = Objects.toString(Objects.requireNonNull(properties.get("tablePrefix"), "tablePrefix required "));
        this.beginIndex = Integer.parseInt(Objects.toString(Objects.requireNonNull(properties.get("beginIndex"), "beginIndex required ")));
        this.endIndex = Integer.parseInt(Objects.toString(Objects.requireNonNull(properties.get("endIndex"), "endIndex required ")));
        this.columnName=  Objects.toString(properties.get("columnName"));
        String targetName =  Objects.toString(Objects.requireNonNull(properties.get("targetName")));
        String schema = Objects.toString(Objects.requireNonNull(properties.get("schemaName")));
        String table =  Objects.toString(Objects.requireNonNull(properties.get("tableName")));

        this.segmentQuery = Boolean.parseBoolean( Objects.toString(properties.getOrDefault("segmentQuery", Boolean.FALSE.toString())));

        this.defaultDataNode = new DataNode() {
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
        };
    }

    @Override
    public boolean isShardingKey(String name) {
        return this.columnName.equals(name);
    }

    @Override
    public String getUniqueID() {
        return defaultDataNode+tablePrefix+beginIndex+endIndex+segmentQuery;
    }

    private DataNode getDataNode(String tableName) {
        return new DataNode() {
            @Override
            public String getTargetName() {
                return defaultDataNode.getTargetName();
            }

            @Override
            public String getSchema() {
                return defaultDataNode.getSchema();
            }

            @Override
            public String getTable() {
                return tableName;
            }
        };
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction==null)return false;
        if (MergeSubTablesFunction.class.isAssignableFrom(customRuleFunction.getClass())){
            MergeSubTablesFunction tablesFunction = (MergeSubTablesFunction)customRuleFunction;
            DataNode defaultDataNode  = tablesFunction.defaultDataNode;
            String tablePrefix = tablesFunction.tablePrefix;
            int beginIndex = tablesFunction.beginIndex;
            int endIndex = tablesFunction.endIndex;
            boolean segmentQuery = tablesFunction.segmentQuery;
            return Objects.equals(this.defaultDataNode,defaultDataNode)&&
                    Objects.equals(this.tablePrefix,tablePrefix)&&
                    Objects.equals(this.beginIndex,beginIndex)&&
                    Objects.equals(this.endIndex,endIndex)&&
                    Objects.equals(this.segmentQuery,segmentQuery);
        }

        return super.isSameDistribution(customRuleFunction);
    }
}