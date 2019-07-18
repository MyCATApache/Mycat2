package io.mycat.config.datasource;

import io.mycat.config.ConfigurableRoot;
import io.mycat.config.YamlUtil;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class JdbcDriverRootConfig extends ConfigurableRoot {
  Map<String,String> jdbcDriver;
  String datasourceProviderClass;
  public  Map<String,String> getJdbcDriver() {
    return jdbcDriver;
  }

  public void setJdbcDriver(Map<String, String> jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
  }

  public String getDatasourceProviderClass() {
    return datasourceProviderClass;
  }

  public void setDatasourceProviderClass(String datasourceProviderClass) {
    this.datasourceProviderClass = datasourceProviderClass;
  }

  public static void main(String[] args) throws Exception {
    JdbcDriverRootConfig config = new JdbcDriverRootConfig();
    config.setDatasourceProviderClass("io.mycat.datasource.jdbc.DruidDatasourceProvider");
    Map<String,String> map = new HashMap<>();
    map.put("mysql","com.mysql.jdbc.Driver");
    config.setJdbcDriver(map);
    String dump = YamlUtil.dump(config);
  }
}