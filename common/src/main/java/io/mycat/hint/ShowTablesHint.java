package io.mycat.hint;

public class ShowTablesHint extends HintBuilder {
    public void setGlobalType() {
        setType("global");
    }

    public void setShardingType() {
        setType("sharding");
    }

    public void setNormalType() {
        setType("normal");
    }

    public void setCustomType() {
        setType("custom");
    }

    public void setType(String name) {
        map.put("type", name);
    }

    public void setSchemaName(String name) {
        map.put("schemaName", name);
    }

    @Override
    public String getCmd() {
        return "showTables";
    }
}