package io.mycat.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Data
public class DatasourceRootConfig {
    private String datasourceProviderClass;
    private List<DatasourceConfig> datasources = new ArrayList<>();
    private TimerConfig timer = new TimerConfig();

    @Data
    public static class DatasourceConfig {
        private String name;
        private String ip;
        private int port;
        private String user;
        private String password;
        private int maxCon = 1000;
        private int minCon = 1;
        private int maxRetryCount = 5;
        private long maxConnectTimeout = 3 * 1000;
        private String dbType;
        private String url;
        private int weight = 0;
        private List<String> initSqls;
        private boolean initSqlsGetConnection;
        private String instanceType;
        private String jdbcDriverClass;
        private long idleTimeout = TimeUnit.SECONDS.toMillis(60);

        public List<String> getInitSqls() {
            if (initSqls == null) initSqls = Collections.emptyList();
            return initSqls;
        }

        public boolean isMySQLType() {
            return this.getDbType() == null || this.getDbType().toUpperCase().contains("MYSQL");
        }

        public boolean isJdbcType() {
            return getUrl() != null;
        }

    }

}