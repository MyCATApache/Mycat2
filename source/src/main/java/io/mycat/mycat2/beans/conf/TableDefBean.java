package io.mycat.mycat2.beans.conf;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc:
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class TableDefBean {
    public enum TableTypeEnum {
        MASTER, SLAVE;
    }

    public enum TypeEnum {
        global;
    }

    private String name;
    private TableTypeEnum tableType;
    private String shardingKey;
    private String shardingRule;
    private String store;
    private String dataNode;
    /**
     * type=global为全局表，否则为普通表
     */
    private TypeEnum type;
    private List<String> dataNodes = new ArrayList<String>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TableTypeEnum getTableType() {
        return tableType;
    }

    public void setTableType(TableTypeEnum tableType) {
        this.tableType = tableType;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public void setShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
    }

    public String getShardingRule() {
        return shardingRule;
    }

    public void setShardingRule(String shardingRule) {
        this.shardingRule = shardingRule;
    }

    public String getStore() {
        return store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

    public TypeEnum getType() {
        return type;
    }

    public void setType(TypeEnum type) {
        this.type = type;
    }

    public List<String> getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(List<String> dataNodes) {
        this.dataNodes = dataNodes;
    }

    @Override
    public String toString() {
        return "TableDefBean [name=" + name + ", tableType=" + tableType + ", shardingKey="
                + shardingKey + ", shardingRule=" + shardingRule + ", store=" + store
                + ", dataNode=" + dataNode + ", type=" + type + "]";
    }

}
