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
import java.util.List;
import java.util.Map;

@Data
public class BinlogHint extends HintBuilder {
    String name;
    List<Input> inputs;

    @Data
    public static class Input {
        String url;
        String username;
        String password;
        List<String> inputTableNames;
        List<String> outputTableNames;
    }

    public static BinlogHint create(String name, List<Input> inputs) {
        BinlogHint migrateHint = new BinlogHint();
        migrateHint.setName(name);
        migrateHint.setInputs(inputs);
        return migrateHint;
    }

    @Override
    public String getCmd() {
        return "BINLOG";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(this));
    }
}