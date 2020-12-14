package io.mycat.config;

import com.alibaba.druid.DbType;
import com.alibaba.druid.util.JdbcConstants;
import com.mysql.cj.conf.ConnectionUrlParser;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode
public class DatasourceConfig {
    private String name;
    private String user;
    private String password;
    private int maxCon = 1000;
    private int minCon = 1;
    private int maxRetryCount = 5;
    private long maxConnectTimeout = 3 * 1000;
    private String dbType = null;
    private String url;
    private int weight = 0;
    private List<String> initSqls;
    private boolean initSqlsGetConnection = true;
    private String instanceType = "READ_WRITE";
    private long idleTimeout = TimeUnit.SECONDS.toMillis(60);
    private String jdbcDriverClass;//保留属性
    private String type = DatasourceType.JDBC.name();

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

    public void setUrl(String url) {
        if ("mysql".equalsIgnoreCase(getDbType())) {
            ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(url);
            Map<String, String> properties = new HashMap<>(connectionUrlParser.getProperties());
            if (!properties.containsKey("useUnicode")) {
                properties.put("useUnicode", "true");
            }
            if (!properties.containsKey("characterEncoding")) {
                properties.put("characterEncoding", "UTF-8");
            }
            if (!properties.containsKey("serverTimezone")) {
                properties.put("serverTimezone", "UTC");
            }
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
            DbType dbTypeRaw = getDbTypeRaw(getUrl());
            if (dbTypeRaw != null) {
                dbType = dbTypeRaw.name();
            }
        }
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = Objects.requireNonNull(dbType, "dbType is null");
    }

    public static DbType getDbTypeRaw(String rawUrl) {
        if (rawUrl == null) {
            return null;
        }

        if (rawUrl.startsWith("jdbc:derby:") || rawUrl.startsWith("jdbc:log4jdbc:derby:")) {
            return DbType.derby;
        } else if (rawUrl.startsWith("jdbc:mysql:") || rawUrl.startsWith("jdbc:cobar:")
                || rawUrl.startsWith("jdbc:log4jdbc:mysql:")) {
            return DbType.mysql;
        } else if (rawUrl.startsWith("jdbc:mariadb:")) {
            return DbType.mariadb;
        } else if (rawUrl.startsWith("jdbc:oracle:") || rawUrl.startsWith("jdbc:log4jdbc:oracle:")) {
            return DbType.oracle;
        } else if (rawUrl.startsWith("jdbc:alibaba:oracle:")) {
            return DbType.ali_oracle;
        } else if (rawUrl.startsWith("jdbc:oceanbase:")) {
            return DbType.oceanbase;
        } else if (rawUrl.startsWith("jdbc:oceanbase:oracle:")) {
            return DbType.oceanbase_oracle;
        } else if (rawUrl.startsWith("jdbc:microsoft:") || rawUrl.startsWith("jdbc:log4jdbc:microsoft:")) {
            return DbType.sqlserver;
        } else if (rawUrl.startsWith("jdbc:sqlserver:") || rawUrl.startsWith("jdbc:log4jdbc:sqlserver:")) {
            return DbType.sqlserver;
        } else if (rawUrl.startsWith("jdbc:sybase:Tds:") || rawUrl.startsWith("jdbc:log4jdbc:sybase:")) {
            return DbType.sybase;
        } else if (rawUrl.startsWith("jdbc:jtds:") || rawUrl.startsWith("jdbc:log4jdbc:jtds:")) {
            return DbType.jtds;
        } else if (rawUrl.startsWith("jdbc:fake:") || rawUrl.startsWith("jdbc:mock:")) {
            return DbType.mock;
        } else if (rawUrl.startsWith("jdbc:postgresql:") || rawUrl.startsWith("jdbc:log4jdbc:postgresql:")) {
            return DbType.postgresql;
        } else if (rawUrl.startsWith("jdbc:edb:")) {
            return DbType.edb;
        } else if (rawUrl.startsWith("jdbc:hsqldb:") || rawUrl.startsWith("jdbc:log4jdbc:hsqldb:")) {
            return DbType.hsql;
        } else if (rawUrl.startsWith("jdbc:odps:")) {
            return DbType.odps;
        } else if (rawUrl.startsWith("jdbc:db2:")) {
            return DbType.db2;
        } else if (rawUrl.startsWith("jdbc:sqlite:")) {
            return DbType.sqlite;
        } else if (rawUrl.startsWith("jdbc:ingres:")) {
            return DbType.ingres;
        } else if (rawUrl.startsWith("jdbc:h2:") || rawUrl.startsWith("jdbc:log4jdbc:h2:")) {
            return DbType.h2;
        } else if (rawUrl.startsWith("jdbc:mckoi:")) {
            return DbType.mock;
        } else if (rawUrl.startsWith("jdbc:cloudscape:")) {
            return DbType.cloudscape;
        } else if (rawUrl.startsWith("jdbc:informix-sqli:") || rawUrl.startsWith("jdbc:log4jdbc:informix-sqli:")) {
            return DbType.informix;
        } else if (rawUrl.startsWith("jdbc:timesten:")) {
            return DbType.timesten;
        } else if (rawUrl.startsWith("jdbc:as400:")) {
            return DbType.as400;
        } else if (rawUrl.startsWith("jdbc:sapdb:")) {
            return DbType.sapdb;
        } else if (rawUrl.startsWith("jdbc:JSQLConnect:")) {
            return DbType.JSQLConnect;
        } else if (rawUrl.startsWith("jdbc:JTurbo:")) {
            return DbType.JTurbo;
        } else if (rawUrl.startsWith("jdbc:firebirdsql:")) {
            return DbType.firebirdsql;
        } else if (rawUrl.startsWith("jdbc:interbase:")) {
            return DbType.interbase;
        } else if (rawUrl.startsWith("jdbc:pointbase:")) {
            return DbType.pointbase;
        } else if (rawUrl.startsWith("jdbc:edbc:")) {
            return DbType.edbc;
        } else if (rawUrl.startsWith("jdbc:mimer:multi1:")) {
            return DbType.mimer;
        } else if (rawUrl.startsWith("jdbc:dm:")) {
            return JdbcConstants.DM;
        } else if (rawUrl.startsWith("jdbc:kingbase:") || rawUrl.startsWith("jdbc:kingbase8:")) {
            return JdbcConstants.KINGBASE;
        } else if (rawUrl.startsWith("jdbc:gbase:")) {
            return JdbcConstants.GBASE;
        } else if (rawUrl.startsWith("jdbc:xugu:")) {
            return JdbcConstants.XUGU;
        } else if (rawUrl.startsWith("jdbc:log4jdbc:")) {
            return DbType.log4jdbc;
        } else if (rawUrl.startsWith("jdbc:hive:")) {
            return DbType.hive;
        } else if (rawUrl.startsWith("jdbc:hive2:")) {
            return DbType.hive;
        } else if (rawUrl.startsWith("jdbc:phoenix:")) {
            return DbType.phoenix;
        } else if (rawUrl.startsWith("jdbc:kylin:")) {
            return DbType.kylin;
        } else if (rawUrl.startsWith("jdbc:elastic:")) {
            return DbType.elastic_search;
        } else if (rawUrl.startsWith("jdbc:clickhouse:")) {
            return DbType.clickhouse;
        } else if (rawUrl.startsWith("jdbc:presto:")) {
            return DbType.presto;
        } else if (rawUrl.startsWith("jdbc:inspur:")) {
            return DbType.kdb;
        } else if (rawUrl.startsWith("jdbc:polardb")) {
            return DbType.polardb;
        } else {
            return null;
        }
    }
}