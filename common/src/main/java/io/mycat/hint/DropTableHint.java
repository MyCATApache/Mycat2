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
import java.util.Map;
import java.util.Objects;

public class DropTableHint extends HintBuilder {
    public static String create(String schemaName,String tableName) {

        DropTableHint dropTableHint = new DropTableHint();
        dropTableHint.map.put("schemaName", Objects.requireNonNull(schemaName));
        dropTableHint.map.put("tableName",Objects.requireNonNull(tableName));
        return dropTableHint.build();
    }

    @Override
    public String getCmd() {
        return "dropTable";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(map));
    }
}