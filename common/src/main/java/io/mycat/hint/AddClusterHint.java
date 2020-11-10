package io.mycat.hint;

import io.mycat.config.ClusterConfig;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.List;

public  class AddClusterHint extends HintBuilder {
        private ClusterConfig config;

        public static String create(String name, List<String> dsNames, List<String> ss) {
            ClusterConfig clusterConfig = new ClusterConfig();
            clusterConfig.setName(name);
            clusterConfig.setMasters(dsNames);
            clusterConfig.setReplicas(ss);

            AddClusterHint addClusterHint = new AddClusterHint();
            addClusterHint.setConfig(clusterConfig);

            return addClusterHint.build();
        }


        public void setConfig(ClusterConfig config) {
            this.config = config;
        }

        @Override
        public String getCmd() {
            return "addCluster";
        }

        @Override
        public String build() {
            return MessageFormat.format("/*! mycat:{0}{1} */;",
                    getCmd(),
                    JsonUtil.toJson(config));
        }

        public static AddClusterHint create(ClusterConfig clusterConfig) {
            AddClusterHint addClusterHint = new AddClusterHint();
            addClusterHint.setConfig(clusterConfig);
            return addClusterHint;
        }
    }