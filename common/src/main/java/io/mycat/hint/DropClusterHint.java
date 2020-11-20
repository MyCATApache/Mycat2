package io.mycat.hint;

import io.mycat.config.ClusterConfig;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.List;

public class DropClusterHint extends HintBuilder {
    private ClusterConfig config;

    public static String create(String name) {
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setName(name);
        DropClusterHint dropClusterHint = new DropClusterHint();
        dropClusterHint.setConfig(clusterConfig);

        return dropClusterHint.build();
    }


    public void setConfig(ClusterConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "dropCluster";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*! mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }

    public static DropClusterHint create(ClusterConfig clusterConfig) {
        DropClusterHint dropClusterHint = new DropClusterHint();
        dropClusterHint.setConfig(clusterConfig);
        return dropClusterHint;
    }
}