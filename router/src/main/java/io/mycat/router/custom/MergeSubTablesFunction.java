package io.mycat.router.custom;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


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

    @Override
    public String name() {
        return "MergeSubTables";
    }

    @Override
    public DataNode calculate(String columnValue) {
        if (columnValue == null) {
            return defaultDataNode;
        }
        String tableName = tablePrefix + columnValue.substring(beginIndex, endIndex);
        return getDataNode(tableName);
    }


    @Override
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
    protected void init(ShardingTableHandler tableHandler, Map<String, String> properties, Map<String, String> ranges) {

        LOGGER.debug("properties:{}", properties);
        LOGGER.debug("ranges:{}", ranges);

        this.tablePrefix = Objects.requireNonNull(properties.get("tablePrefix"), "tablePrefix required ");
        this.beginIndex = Integer.parseInt(Objects.requireNonNull(properties.get("beginIndex"), "beginIndex required "));
        this.endIndex = Integer.parseInt(Objects.requireNonNull(properties.get("endIndex"), "endIndex required "));

        String targetName = Objects.requireNonNull(properties.get("targetName"));
        String schema = Objects.requireNonNull(properties.get("schemaName"));
        String table = Objects.requireNonNull(properties.get("tableName"));

        this.segmentQuery = Boolean.parseBoolean(properties.getOrDefault("segmentQuery", Boolean.FALSE.toString()));

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
}