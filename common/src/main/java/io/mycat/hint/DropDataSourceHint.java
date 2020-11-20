package io.mycat.hint;

import io.mycat.config.DatasourceConfig;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;

public  class DropDataSourceHint extends HintBuilder {
        private DatasourceConfig config;

        public void setDatasourceConfig(DatasourceConfig config) {
            this.config = config;
        }

        @Override
        public String getCmd() {
            return "dropDataSource";
        }

        @Override
        public String build() {
            return MessageFormat.format("/*+ mycat:{0}{1} */;",
                    getCmd(),
                    JsonUtil.toJson(config));
        }

        public static String create(String name) {
            DropDataSourceHint dropDataSourceHint = new DropDataSourceHint();
            DatasourceConfig datasourceConfig = new DatasourceConfig();
            dropDataSourceHint.setDatasourceConfig(datasourceConfig);
            datasourceConfig.setName(name);
            return dropDataSourceHint.build();
        }

    }