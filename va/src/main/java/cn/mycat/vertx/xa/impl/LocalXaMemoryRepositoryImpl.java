package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.MySQLManager;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LocalXaMemoryRepositoryImpl extends MemoryRepositoryImpl {
    private final static Logger LOGGER = LoggerFactory.getLogger(LocalXaMemoryRepositoryImpl.class);
    private Supplier<MySQLManager> mySQLManagerSupplier;
    public static final String database = "mycat";
    public static final  String tableName = "xa_log";
    public LocalXaMemoryRepositoryImpl(Supplier<MySQLManager> mySQLManagerSupplier) {
        this.mySQLManagerSupplier = mySQLManagerSupplier;
    }

    public static Future<Void> tryCreateLogTable(SqlConnection dataSource) {


        String createDatabaseSQL = "create database if not exists `" + database + "`";
        String createTableSQL = "create table if not exists `" + database + "`." + "`" + tableName + "`"
                + "(`xid` varchar(64) NOT NULL,\n" +
                "`state` varchar(128) NOT NULL,\n" +
                "`expires` int(64) NOT NULL,\n" +
                "`info` varchar(128) NOT NULL,\n" +
                "PRIMARY KEY (`xid`),\n" +
                "UNIQUE KEY `uk_key` (`xid`))ENGINE=InnoDB DEFAULT";
        return dataSource.query(createDatabaseSQL
                + ";" + createTableSQL).execute().mapEmpty();
    }

    @Override
    public void init() {
        super.init();
        MySQLManager mySQLManager = mySQLManagerSupplier.get();
        Future<Map<String, SqlConnection>> mapFuture = mySQLManager.getConnectionMap();
        Future<Void> future = mapFuture
                .flatMap(stringSqlConnectionMap -> CompositeFuture.all(stringSqlConnectionMap.values().stream()
                        .map(connection -> tryCreateLogTable(connection)).collect(Collectors.toList()))
                        .onComplete(event -> stringSqlConnectionMap.values().forEach(c -> c.close())).mapEmpty());
    }
    @Override
    public Future<Collection<ImmutableCoordinatorLog>> getCoordinatorLogsForRecover() {
        MySQLManager mySQLManager = mySQLManagerSupplier.get();
        Future<Map<String, SqlConnection>> mapFuture = mySQLManager.getConnectionMap();
        LinkedBlockingQueue<ImmutableCoordinatorLog> objects = new LinkedBlockingQueue<>();
        Future<Void> future = mapFuture
                .flatMap(stringSqlConnectionMap -> CompositeFuture.all(stringSqlConnectionMap.values().stream()
                        .map(connection ->{
                                    Future<Object> future1 = connection.query("select * from " + database + "." + tableName)
                                            .execute()
                                            .map(rows -> {
                                                for (Row row : rows) {
                                                    objects.add(ImmutableCoordinatorLog.from(row.getString("info")));
                                                }
                                                return null;
                                            }).mapEmpty();
                                    return future1;
                                }
                                ).collect(Collectors.toList()))
                        .onComplete(event -> stringSqlConnectionMap.values().forEach(c -> c.close())).mapEmpty());
        return future.map(objects);
    }

    @Override
    public void close() {

    }
}
