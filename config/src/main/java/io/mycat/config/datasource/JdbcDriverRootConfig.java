package io.mycat.config.datasource;

import io.mycat.config.ConfigurableRoot;
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
}