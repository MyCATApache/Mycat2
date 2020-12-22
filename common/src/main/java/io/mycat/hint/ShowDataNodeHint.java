package io.mycat.hint;

import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.HashMap;

public class ShowDataNodeHint extends HintBuilder {
    private String schemaName;
    private String tableName;

    public static String create(
            String schemaName,
            String tableName) {
        ShowDataNodeHint showDataNodeHint = new ShowDataNodeHint();
        showDataNodeHint.setSchemaName(schemaName);
        showDataNodeHint.setTableName(tableName);
        return showDataNodeHint.build();
    }

    @Override
    public String getCmd() {
        return "showDataNodes";
    }

    @Override
    public String build() {
        HashMap<String, String> map = new HashMap<>();
        map.put("schemaName", schemaName);
        map.put("tableName", tableName);
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(map));
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}