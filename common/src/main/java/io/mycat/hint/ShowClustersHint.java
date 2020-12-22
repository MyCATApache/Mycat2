package io.mycat.hint;

public class ShowClustersHint extends HintBuilder {

    public void setName(String name) {
        map.put("name", name);
    }

    @Override
    public String getCmd() {
        return "showClusters";
    }
}
