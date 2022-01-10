package io.mycat.config;


import com.mysql.cj.conf.ConnectionUrlParser;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode
public class DatasourceConfig implements KVObject{

    private String name;


    private String user;
    private String password;
    private int maxCon = 1000;
    private int minCon = 1;
    private int maxRetryCount = 5;
    private long maxConnectTimeout = 30 * 1000;

    private String dbType = "mysql";

    private String url;
    private int weight = 0;
    private List<String> initSqls;
    private boolean initSqlsGetConnection = true;
    private String instanceType = "READ_WRITE";
    private long idleTimeout = TimeUnit.SECONDS.toMillis(60);
    private String jdbcDriverClass;//保留属性
    @javax.validation.constraints.NotNull
    private String type = DatasourceType.JDBC.name();
    private int queryTimeout = 0;

    public DatasourceConfig() {
    }

    public static String getDbTypeRaw(String rawUrl) {
        if (rawUrl == null) {
            return null;
        }

        if (rawUrl.startsWith("jdbc:derby:") || rawUrl.startsWith("jdbc:log4jdbc:derby:")) {
            return "derby";
        } else if (rawUrl.startsWith("jdbc:mysql:") || rawUrl.startsWith("jdbc:cobar:")
                || rawUrl.startsWith("jdbc:log4jdbc:mysql:")) {
            return "mysql";
        } else if (rawUrl.startsWith("jdbc:mariadb:")) {
            return "mariadb";
        } else if (rawUrl.startsWith("jdbc:oracle:") || rawUrl.startsWith("jdbc:log4jdbc:oracle:")) {
            return "oracle";
        } else if (rawUrl.startsWith("jdbc:alibaba:oracle:")) {
            return "ali_oracle";
        } else if (rawUrl.startsWith("jdbc:oceanbase:")) {
            return "oceanbase";
        } else if (rawUrl.startsWith("jdbc:oceanbase:oracle:")) {
            return "oceanbase_oracle";
        } else if (rawUrl.startsWith("jdbc:microsoft:") || rawUrl.startsWith("jdbc:log4jdbc:microsoft:")) {
            return "sqlserver";
        } else if (rawUrl.startsWith("jdbc:sqlserver:") || rawUrl.startsWith("jdbc:log4jdbc:sqlserver:")) {
            return "sqlserver";
        } else if (rawUrl.startsWith("jdbc:sybase:Tds:") || rawUrl.startsWith("jdbc:log4jdbc:sybase:")) {
            return "sybase";
        } else if (rawUrl.startsWith("jdbc:jtds:") || rawUrl.startsWith("jdbc:log4jdbc:jtds:")) {
            return "jtds";
        } else if (rawUrl.startsWith("jdbc:fake:") || rawUrl.startsWith("jdbc:mock:")) {
            return "mock";
        } else if (rawUrl.startsWith("jdbc:postgresql:") || rawUrl.startsWith("jdbc:log4jdbc:postgresql:")) {
            return "postgresql";
        } else if (rawUrl.startsWith("jdbc:edb:")) {
            return "edb";
        } else if (rawUrl.startsWith("jdbc:hsqldb:") || rawUrl.startsWith("jdbc:log4jdbc:hsqldb:")) {
            return "hsql";
        } else if (rawUrl.startsWith("jdbc:odps:")) {
            return "odps";
        } else if (rawUrl.startsWith("jdbc:db2:")) {
            return "db2";
        } else if (rawUrl.startsWith("jdbc:sqlite:")) {
            return "sqlite";
        } else if (rawUrl.startsWith("jdbc:ingres:")) {
            return "ingres";
        } else if (rawUrl.startsWith("jdbc:h2:") || rawUrl.startsWith("jdbc:log4jdbc:h2:")) {
            return "h2";
        } else if (rawUrl.startsWith("jdbc:mckoi:")) {
            return "mock";
        } else if (rawUrl.startsWith("jdbc:cloudscape:")) {
            return "cloudscape";
        } else if (rawUrl.startsWith("jdbc:informix-sqli:") || rawUrl.startsWith("jdbc:log4jdbc:informix-sqli:")) {
            return "informix";
        } else if (rawUrl.startsWith("jdbc:timesten:")) {
            return "timesten";
        } else if (rawUrl.startsWith("jdbc:as400:")) {
            return "as400";
        } else if (rawUrl.startsWith("jdbc:sapdb:")) {
            return "sapdb";
        } else if (rawUrl.startsWith("jdbc:JSQLConnect:")) {
            return "JSQLConnect";
        } else if (rawUrl.startsWith("jdbc:JTurbo:")) {
            return "JTurbo";
        } else if (rawUrl.startsWith("jdbc:firebirdsql:")) {
            return "firebirdsql";
        } else if (rawUrl.startsWith("jdbc:interbase:")) {
            return "interbase";
        } else if (rawUrl.startsWith("jdbc:pointbase:")) {
            return "pointbase";
        } else if (rawUrl.startsWith("jdbc:edbc:")) {
            return "edbc";
        } else if (rawUrl.startsWith("jdbc:mimer:multi1:")) {
            return "mimer";
        } else if (rawUrl.startsWith("jdbc:dm:")) {
            return "dm";
        } else if (rawUrl.startsWith("jdbc:kingbase:") || rawUrl.startsWith("jdbc:kingbase8:")) {
            return "kingbase";
        } else if (rawUrl.startsWith("jdbc:gbase:")) {
            return "gbase";
        } else if (rawUrl.startsWith("jdbc:xugu:")) {
            return "xugu";
        } else if (rawUrl.startsWith("jdbc:log4jdbc:")) {
            return "log4jdbc";
        } else if (rawUrl.startsWith("jdbc:hive:")) {
            return "hive";
        } else if (rawUrl.startsWith("jdbc:hive2:")) {
            return "hive";
        } else if (rawUrl.startsWith("jdbc:phoenix:")) {
            return "phoenix";
        } else if (rawUrl.startsWith("jdbc:kylin:")) {
            return "kylin";
        } else if (rawUrl.startsWith("jdbc:elastic:")) {
            return "elastic_search";
        } else if (rawUrl.startsWith("jdbc:clickhouse:")) {
            return "clickhouse";
        } else if (rawUrl.startsWith("jdbc:presto:")) {
            return "presto";
        } else if (rawUrl.startsWith("jdbc:inspur:")) {
            return "kdb";
        } else if (rawUrl.startsWith("jdbc:polardb")) {
            return "polardb";
        } else {
            return null;
        }
    }
//
//        public boolean isMySQLType() {
//            return this.getDbType() == null || this.getDbType().toUpperCase().contains("MYSQL");
//        }
//
//        public boolean isJdbcType() {
//            return getUrl() != null;
//        }

    public List<String> getInitSqls() {
        if (initSqls == null) initSqls = Collections.emptyList();
        return initSqls;
    }

    public DatasourceType computeType() {
        return DatasourceType.valueOf(type);
    }

    public void setUrl(String url) {
        if ("mysql".equalsIgnoreCase(getDbType()) && url != null) {
            ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(url);
            Map<String, String> properties = new HashMap<>(connectionUrlParser.getProperties());
            if (!properties.containsKey("useUnicode")) {
                properties.put("useUnicode", "true");
            }
            if (!properties.containsKey("characterEncoding")) {
                properties.put("characterEncoding", "UTF-8");
            }
            if (!properties.containsKey("serverTimezone")) {
                TimeZone timeZone = TimeZone.getDefault();
                timeZone.getID();
                properties.put("serverTimezone", "Asia/Shanghai");
            }
//            if (!properties.containsKey("useSSL")) {
//                properties.put("useSSL", "false");
//            }
            if (!properties.containsKey("autoReconnect")) {
                properties.put("autoReconnect", "true");
            }
//            if (!properties.containsKey("AllowPublicKeyRetrieval")) {
//                properties.put("AllowPublicKeyRetrieval", "true");
//            }
            int i = url.indexOf('?');
            if (i == -1) {
                url += "?";
            } else {
                url = url.substring(0, i + 1);
            }
            url += properties.entrySet().stream().map(j -> j.getKey() + "=" + j.getValue())
                    .collect(Collectors.joining("&"));
        }
        this.url = url;
    }

    public String getDbType() {
        if (dbType == null) {
            String dbTypeRaw = getDbTypeRaw(getUrl());
            if (dbTypeRaw != null) {
                dbType = dbTypeRaw;
            }
        }
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = Objects.requireNonNull(dbType, "dbType is null");
    }

    @Override
    public String keyName() {
        return name;
    }
    @Override
    public String path() {
        return "datasources";
    }

    @Override
    public String fileName() {
        return "datasource";
    }

    public static enum DatasourceType {
        NATIVE(true, true),
        JDBC(false, true),
        NATIVE_JDBC(true, true);
        boolean isJdbc;
        boolean isNative;

        DatasourceType(boolean isNative, boolean isJdbc) {
            this.isNative = isNative;
            this.isJdbc = isJdbc;

        }

        public boolean isNative() {
            return this.isNative;
        }

        public boolean isJdbc() {
            return this.isJdbc;
        }
    }
}