package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Data
@EqualsAndHashCode
public class DatasourceRootConfig {
    private String datasourceProviderClass;
    private List<DatasourceConfig> datasources = new ArrayList<>();
    private TimerConfig timer = new TimerConfig();

    @Data
    @EqualsAndHashCode
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
        private long idleTimeout = TimeUnit.SECONDS.toMillis(60);
        private String jdbcDriverClass;//保留属性
        private String type = DatasourceType.NATIVE_JDBC.name();

        public List<String> getInitSqls() {
            if (initSqls == null) initSqls = Collections.emptyList();
            return initSqls;
        }
//
//        public boolean isMySQLType() {
//            return this.getDbType() == null || this.getDbType().toUpperCase().contains("MYSQL");
//        }
//
//        public boolean isJdbcType() {
//            return getUrl() != null;
//        }

        public DatasourceType computeType() {
            return DatasourceType.valueOf(type);
        }

        public static enum DatasourceType {
            NATIVE(true,false),
            JDBC(false,true),
            NATIVE_JDBC(true,true);
            boolean isJdbc;
            boolean isNative;
            DatasourceType(boolean isNative,boolean isJdbc) {
                this.isNative = isNative;
                this.isJdbc = isJdbc;

            }
            public boolean isNative(){
                return this.isNative;
            }

            public  boolean isJdbc(){
                return this.isJdbc;
            }
        }

    }

}