package io.mycat.hint;

public class ShowSchemasHint extends HintBuilder {
    public void setSchemaName(String name) {
        map.put("schemaName", name);
    }

    @Override
    public String getCmd() {
        return "showSchemas";
    }
}
