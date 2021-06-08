/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hint;

import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.HashMap;

public class ShowTopologyHint extends HintBuilder {
    private String schemaName;
    private String tableName;

    public static String create(
            String schemaName,
            String tableName) {
        ShowTopologyHint showDataNodeHint = new ShowTopologyHint();
        showDataNodeHint.setSchemaName(schemaName);
        showDataNodeHint.setTableName(tableName);
        return showDataNodeHint.build();
    }

    @Override
    public String getCmd() {
        return "showTopology";
    }

    @Override
    public String build() {
        HashMap<String, String> map = new HashMap<>();
        map.put("schemaName", schemaName);
        map.put("tableName", tableName);
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(map));
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public static void main(String[] args) {
        System.out.println(ShowTopologyHint.create("db1","travelrecord"));
    }
}