package io.mycat.config;

import io.mycat.config.datasource.DatasourceConfig;

public class MycatConfigUtil {

  public static boolean isMySQLType(DatasourceConfig config) {
    return config.getDbType() == null || config.getDbType().toUpperCase().contains("MYSQL");
  }
}