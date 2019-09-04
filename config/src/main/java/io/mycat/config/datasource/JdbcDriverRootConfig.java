package io.mycat.config.datasource;

import io.mycat.config.ConfigurableRoot;
import io.mycat.config.YamlUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JdbcDriverRootConfig extends ConfigurableRoot {

  String datasourceProviderClass;
  int minThread;
  int maxThread;
  int waitTaskTimeout;
  String timeUnit;
  int maxPengdingLimit;

  public JdbcDriverRootConfig() {
    this.datasourceProviderClass = "io.mycat.datasource.jdbc.datasourceProvider.AtomikosDatasourceProvider";
    this.minThread = 2;
    this.maxThread = 32;
    this.waitTaskTimeout = 5;
    this.timeUnit = TimeUnit.SECONDS.name();
    this.maxPengdingLimit = -1;
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
    Map<String, String> map = new HashMap<>();
    map.put("mysql", "com.mysql.jdbc.Driver");
    String dump = YamlUtil.dump(config);
  }

  public int getMinThread() {
    return minThread;
  }

  public void setMinThread(int minThread) {
    this.minThread = minThread;
  }

  public int getMaxThread() {
    return maxThread;
  }

  public void setMaxThread(int maxThread) {
    this.maxThread = maxThread;
  }

  public int getWaitTaskTimeout() {
    return waitTaskTimeout;
  }

  public void setWaitTaskTimeout(int waitTaskTimeout) {
    this.waitTaskTimeout = waitTaskTimeout;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(String timeUnit) {
    this.timeUnit = timeUnit;
  }

  public int getMaxPengdingLimit() {
    return maxPengdingLimit;
  }

  public void setMaxPengdingLimit(int maxPengdingLimit) {
    this.maxPengdingLimit = maxPengdingLimit;
  }
}