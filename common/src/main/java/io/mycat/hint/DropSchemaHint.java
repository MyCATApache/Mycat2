package io.mycat.hint;

import io.mycat.config.LogicSchemaConfig;
import io.mycat.util.JsonUtil;
import lombok.Data;

import java.text.MessageFormat;

@Data
public class DropSchemaHint extends HintBuilder {
    LogicSchemaConfig config;

    public static String create(String schemaName) {
        DropSchemaHint dropSchemaHint = new DropSchemaHint();
        LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
        logicSchemaConfig.setSchemaName(schemaName);
        dropSchemaHint.setConfig(logicSchemaConfig);
        return dropSchemaHint.build();
    }

    @Override
    public String getCmd() {
        return "DropSchema";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*! mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }
}
