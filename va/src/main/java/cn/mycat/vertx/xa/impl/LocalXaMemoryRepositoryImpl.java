package cn.mycat.vertx.xa.impl;

import com.alibaba.druid.util.JdbcUtils;

import javax.sql.DataSource;
import java.sql.SQLException;

public class LocalXaMemoryRepositoryImpl extends MemoryRepositoryImpl{

    public static void createLogTable(DataSource dataSource) throws SQLException {
        String database = "mycat";
        String tableName = "xa_log";
        JdbcUtils.execute(dataSource,"create database if not exists `"+database+"`");
        JdbcUtils.execute(dataSource,"create table if not exists `"+database+"`."+"`"+tableName+"`"
        +"(`xid` varchar(64) NOT NULL,\n" +
                "`state` varchar(128) NOT NULL,\n" +
                "`expires` int(64) NOT NULL,\n" +
                "`info` varchar(128) NOT NULL,\n" +
                "PRIMARY KEY (`xid`),\n" +
                "UNIQUE KEY `uk_key` (`xid`))ENGINE=InnoDB DEFAULT");
    }
}
