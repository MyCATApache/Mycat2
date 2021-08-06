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

import io.mycat.config.LogicSchemaConfig;
import io.mycat.util.JsonUtil;
import org.jetbrains.annotations.NotNull;

import java.text.MessageFormat;

public class CreateSchemaHint extends HintBuilder {
    private LogicSchemaConfig config;

    public static String create(LogicSchemaConfig config) {
        CreateSchemaHint createSchemaHint = new CreateSchemaHint();
        createSchemaHint.setLogicSchemaConfig(config);
        return createSchemaHint.build();
    }

    public static String create(
            String schemaName,
            String targetName
    ) {
        CreateSchemaHint createSchemaHint = new CreateSchemaHint();
        createSchemaHint.setLogicSchemaConfig(createConfig(schemaName, targetName));
        return createSchemaHint.build();
    }

    @NotNull
    public static LogicSchemaConfig createConfig(String schemaName, String targetName) {
        LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
        schemaConfig.setTargetName(targetName);
        schemaConfig.setSchemaName(schemaName);
        return schemaConfig;
    }

    public static String create(
            String schemaName
    ) {
        return create(schemaName, null);
    }

    public void setLogicSchemaConfig(LogicSchemaConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "createSchema";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }
}