package io.mycat.hint;

import io.mycat.config.LogicSchemaConfig;
import io.mycat.util.JsonUtil;

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
        LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
        schemaConfig.setTargetName(targetName);
        schemaConfig.setSchemaName(schemaName);
        CreateSchemaHint createSchemaHint = new CreateSchemaHint();
        createSchemaHint.setLogicSchemaConfig(schemaConfig);
        return createSchemaHint.build();
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