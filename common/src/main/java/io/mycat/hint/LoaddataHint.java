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