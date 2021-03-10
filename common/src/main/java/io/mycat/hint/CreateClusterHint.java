package io.mycat.hint;

import io.mycat.config.ClusterConfig;
import io.mycat.util.JsonUtil;
import org.jetbrains.annotations.NotNull;

import java.text.MessageFormat;
import java.util.List;

public class CreateClusterHint extends HintBuilder {
    private ClusterConfig config;

    public static String create(String name, List<String> masters, List<String> reads) {
        ClusterConfig clusterConfig = createConfig(name, masters, reads);

        CreateClusterHint createClusterHint = new CreateClusterHint();
        createClusterHint.setConfig(clusterConfig);

        return createClusterHint.build();
    }

    @NotNull
    public static ClusterConfig createConfig(String name, List<String> masters, List<String> reads) {
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setName(name);
        clusterConfig.setMasters(masters);
        clusterConfig.setReplicas(reads);
        return clusterConfig;
    }

    public static CreateClusterHint create(ClusterConfig clusterConfig) {
        CreateClusterHint createClusterHint = new CreateClusterHint();
        createClusterHint.setConfig(clusterConfig);
        return createClusterHint;
    }

    public void setConfig(ClusterConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "createCluster";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }
}