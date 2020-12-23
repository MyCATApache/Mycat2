package io.mycat.hint;

public class showDatasourcesHint extends HintBuilder {

    public void setName(String name) {
        map.put("name", name);
    }

    @Override
    public String getCmd() {
        return "showDatasources";
    }
}