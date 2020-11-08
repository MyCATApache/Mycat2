package io.mycat.config;

import io.mycat.util.YamlUtil;
import lombok.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@EqualsAndHashCode
public class ShardingQueryRootConfig {
    List<LogicSchemaConfig> schemas = new ArrayList<>();
    String prototype;

    public List<LogicSchemaConfig> getSchemas() {
        return schemas;
    }

    public static void main(String[] args) {
        ShardingQueryRootConfig rootConfig = new ShardingQueryRootConfig();
        LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
        logicSchemaConfig.setTargetName("db1");
        logicSchemaConfig.setTargetName("defaultDs");
        rootConfig.getSchemas().add(logicSchemaConfig);
        System.out.println(YamlUtil.dump(rootConfig));
    }

}