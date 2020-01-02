package io.mycat.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DatasourceRootConfig {
    private String datasourceProviderClass;
    private List<DatasourceConfig> datasources = new ArrayList<>();

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
        private long maxConnectTimeout = 3*1000;
        private String dbType;
        private String url;
        private int weight = 0;
        private String initSQL;
        private String initDb;
        private String instanceType;
        private String jdbcDriverClass;


        public boolean isMySQLType() {
            return this.getDbType() == null || this.getDbType().toUpperCase().contains("MYSQL");
        }

        public boolean isJdbcType(){
          return   getUrl()!=null;
        }
    }

}