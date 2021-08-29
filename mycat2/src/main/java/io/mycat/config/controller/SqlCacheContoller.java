package io.mycat.config.controller;

import io.mycat.MetaClusterCurrent;
import io.mycat.commands.SqlResultSetService;
import io.mycat.config.SqlCacheConfig;

import java.util.List;

public class SqlCacheContoller {

    public static void update(List<SqlCacheConfig> sqlCacheConfigList){
        if(MetaClusterCurrent.exist(SqlResultSetService.class)){
            MetaClusterCurrent.wrapper(SqlResultSetService.class).clear();
        }
        SqlResultSetService sqlResultSetService = new SqlResultSetService();
        for (SqlCacheConfig sqlCacheConfig : sqlCacheConfigList) {
            sqlResultSetService.addIfNotPresent(sqlCacheConfig);
        }
        MetaClusterCurrent.register(SqlResultSetService.class,sqlResultSetService);
    }

    public static void add(SqlCacheConfig config){
        SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
        sqlResultSetService.addIfNotPresent(config);
    }
    public static void remove(String name){
        SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
        sqlResultSetService.dropByName(name);
    }
}
