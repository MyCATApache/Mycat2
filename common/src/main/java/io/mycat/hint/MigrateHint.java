/**
 * Copyright (C) <2022>  <chen junwen>
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
import lombok.Data;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

@Data
public class MigrateHint extends HintBuilder {
    String name;
    Input input;
    Output output;

    @Data
    public static class Input {
        String schemaName;
        String tableName;
        String type;


        String url;
        String username;
        String password;
        String sql;
        long count;

        Map<String, String> properties = new HashMap<>();
    }

    @Data
    public static class Output {
        String schemaName;
        String tableName;
        String type;
        String url;
        String username;
        String password;
        Map<String, String> properties = new HashMap<>();
    }

    public static MigrateHint create(String name,Input input, Output output) {
        MigrateHint migrateHint = new MigrateHint();
        migrateHint.setName(name);
        migrateHint.setInput(input);
        migrateHint.setOutput(output);
        return migrateHint;
    }

    @Override
    public String getCmd() {
        return "MIGRATE";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(this));
    }
}