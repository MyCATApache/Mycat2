package io.mycat.config;

import com.mysql.cj.conf.ConnectionUrlParser;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private String dbType = "mysql";
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
       if("mysql".equalsIgnoreCase(getDbType())){
           ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(url);
           Map<String, String> properties = new HashMap<>(connectionUrlParser.getProperties());
           if (!properties.containsKey("useUnicode")){
               properties.put("useUnicode","true");
           }
           if (!properties.containsKey("characterEncoding")){
               properties.put("characterEncoding","UTF-8");
           }
           if (!properties.containsKey("serverTimezone")){
               properties.put("serverTimezone","UTC");
           }
           int i = url.indexOf('?');
           if (i == -1){
               url+="?";
           }else {
               url= url.substring(0,i+1);
           }
           url+=properties.entrySet().stream().map(j->j.getKey()+"="+j.getValue())
                   .collect(Collectors.joining("&"));
       }
        this.url = url;
    }
}