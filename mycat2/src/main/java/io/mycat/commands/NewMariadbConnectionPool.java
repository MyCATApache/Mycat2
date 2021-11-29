package io.mycat.commands;

import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import io.mycat.config.DatasourceConfig;
import io.mycat.newquery.NewMariadbConnectionImpl;
import io.mycat.newquery.NewMycatConnection;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.vertx.core.Future;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.api.MariadbConnection;

import java.time.Duration;

public class NewMariadbConnectionPool implements MycatDatasourcePool{
    final ConnectionPool pool;
    private DatasourceConfig datasourceConfig;
    private String targetName;

    public NewMariadbConnectionPool(DatasourceConfig datasourceConfig,String targetName) {
        this.datasourceConfig = datasourceConfig;
        this.targetName = targetName;

        ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(datasourceConfig.getUrl());
        HostInfo hostInfo = connectionUrlParser.getHosts().get(0);
        String scheme = connectionUrlParser.getScheme();
        MariadbConnectionConfiguration factoryConfig = MariadbConnectionConfiguration
                .builder()
                .host(hostInfo.getHost())
                .port(hostInfo.getPort())
                .username(datasourceConfig.getUser())
                .password(datasourceConfig.getPassword())
                .database(scheme)
                .build();

        MariadbConnectionFactory connFactory = new MariadbConnectionFactory(factoryConfig);

        // Configure Connection Pool
        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration
                .builder(connFactory)
                .maxIdleTime(Duration.ofMillis(datasourceConfig.getIdleTimeout()))
                .maxSize(datasourceConfig.getMaxCon())
                .build();

        this.pool = new ConnectionPool(poolConfig);
    }

   public Future<NewMycatConnection> getConnection(){
        return Future.future(newMycatConnectionPromise -> pool.create().doOnSuccess(connection -> {
            newMycatConnectionPromise.tryComplete(new NewMariadbConnectionImpl((MariadbConnection)connection));
        }).doOnError(throwable -> newMycatConnectionPromise.tryFail(throwable)));
   }

    @Override
    public Integer getAvailableNumber() {
        return pool.getMetrics().map(m->m.idleSize()).orElse(0);
    }

    @Override
    public Integer getUsedNumber() {
        return pool.getMetrics().map(m->m.acquiredSize()).orElse(0);
    }

    @Override
    public String getTargetName() {
        return targetName;
    }

    @Override
    public void close() {
        pool.close();
    }

}
