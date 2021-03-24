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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LoaddataHint extends HintBuilder {
    private Map<String, String> config;

    public LoaddataHint(Map<String, String> config) {
        this.config = config;
    }

    public static String create(
            String schemaName,
            String tableName,
            String fileName
    ) {
        return create(schemaName, tableName, fileName, Collections.emptyMap());
    }

    public static String create(
            String schemaName,
            String tableName,
            String fileName,
            Map<String, String> options
    ) {
        Map<String, String> config = new HashMap<>();
        config.put("schemaName", schemaName);
        config.put("tableName", tableName);
        config.put("fileName", fileName);
        config.putAll(options);
        return new LoaddataHint(config).build();
    }

    @Override
    public String getCmd() {
        return "loaddata";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }
}