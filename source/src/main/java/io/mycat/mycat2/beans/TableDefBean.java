/*
 * Copyright (c) 2016, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.mycat2.beans;

/**
 * Mycat Table def Bean
 *
 * @author wuzhihui
 */
public class TableDefBean {
    public enum TableTypeEnum {
        MASTER, SLAVE;
    }

    private String name;
    private TableTypeEnum type;
    private String shardingKey;
    private String shardingRule;
    private String store;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TableTypeEnum getType() {
        return type;
    }

    public void setType(TableTypeEnum type) {
        this.type = type;
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

    @Override
    public String toString() {
        return "TableDefBean [name=" + name + ", type=" + type + ", store=" + store
                + ", shardingKey=" + shardingKey + ", shardingRule=" + shardingRule + "]";
    }
}
