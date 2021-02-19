package cn.mycat.vertx.xa.impl;

import com.alibaba.druid.util.JdbcUtils;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;

public class LocalXaMemoryRepositoryImpl extends MemoryRepositoryImpl{
    private final static Logger LOGGER = LoggerFactory.getLogger(LocalXaMemoryRepositoryImpl.class);
    public static void tryCreateLogTable(DataSource dataSource){
        String database = "mycat";
        String tableName = "xa_log";

        String createDatabaseSQL = "create database if not exists `" + database + "`";
        String createTableSQL = "create table if not exists `" + database + "`." + "`" + tableName + "`"
                + "(`xid` varchar(64) NOT NULL,\n" +
                "`state` varchar(128) NOT NULL,\n" +
                "`expires` int(64) NOT NULL,\n" +
                "`info` varchar(128) NOT NULL,\n" +
                "PRIMARY KEY (`xid`),\n" +
                "UNIQUE KEY `uk_key` (`xid`))ENGINE=InnoDB DEFAULT";
        try {
            JdbcUtils.execute(dataSource,createDatabaseSQL
                    +";"+createTableSQL);
        } catch (SQLException throwables) {
            LOGGER.error("",throwables);
        }
    }
}
