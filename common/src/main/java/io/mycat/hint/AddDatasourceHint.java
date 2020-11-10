package io.mycat.hint;

import io.mycat.config.DatasourceConfig;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;

public  class AddDatasourceHint extends HintBuilder {
        private DatasourceConfig config;

        public void setDatasourceConfig(DatasourceConfig config) {
            this.config = config;
        }

        @Override
        public String getCmd() {
            return "addDatasource";
        }

        @Override
        public String build() {
            return MessageFormat.format("/*! mycat:{0}{1} */;",
                    getCmd(),
                    JsonUtil.toJson(config));
        }

        public static String create(DatasourceConfig config) {
            AddDatasourceHint addDatasourceHint = new AddDatasourceHint();
            addDatasourceHint.setDatasourceConfig(config);
            return addDatasourceHint.build();
        }

        public static String create(
                String name,
                String url
        ) {
            return create(name, "root", "123456", url);
        }

        public static String create(
                String name,
                String user,
                String password,
                String url
        ) {
            DatasourceConfig datasourceConfig = new DatasourceConfig();
            datasourceConfig.setName(name);
            datasourceConfig.setUrl(url);
            datasourceConfig.setPassword(password);
            datasourceConfig.setUser(user);
            datasourceConfig.setPassword(password);
            return create(datasourceConfig);
        }
    }