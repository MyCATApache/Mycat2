package io.mycat.config;

import io.mycat.util.YamlUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode
public class ShardingQueryRootConfig {
    List<LogicSchemaConfig> schemas = new ArrayList<>();

    public static void main(String[] args) {
        ShardingQueryRootConfig rootConfig = new ShardingQueryRootConfig();
        LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
        logicSchemaConfig.setTargetName("db1");
        logicSchemaConfig.setTargetName("defaultDs");
        rootConfig.getSchemas().add(logicSchemaConfig);
        System.out.println(YamlUtil.dump(rootConfig));
    }

    public List<LogicSchemaConfig> getSchemas() {
        return schemas;
    }

}