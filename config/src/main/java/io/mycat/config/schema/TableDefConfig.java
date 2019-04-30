/**
 * Copyright (C) <2019>  <gaozhiwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.config.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc:
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class TableDefConfig {
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
        return "TableDefConfig [name=" + name + ", tableType=" + tableType + ", shardingKey="
                + shardingKey + ", shardingRule=" + shardingRule + ", store=" + store
                + ", dataNode=" + dataNode + ", type=" + type + "]";
    }

}
