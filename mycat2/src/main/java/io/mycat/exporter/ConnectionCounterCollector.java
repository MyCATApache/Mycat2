package io.mycat.exporter;

import cn.mycat.vertx.xa.MySQLManager;
import com.google.common.collect.ImmutableList;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatServer;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorManager;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.vertx.core.Future;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConnectionCounterCollector extends Collector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCounterCollector.class);

    public ConnectionCounterCollector() {
    }

    @Getter
    public static enum Type {
        TOTAL("total"),
        IDLE("idle"),
        USE("use");

        Type(String name) {
            this.name = name;
        }

        private final String name;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            return ImmutableList.of(countCurrentClient(),
                    countCurrentProxyBackend(),
                    countCurrentJdbcBackend(),
                    countTotalBackend());
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }

    private static GaugeMetricFamily countCurrentClient() {
        MycatServer mycatServer;
        long sum = 0;
        if (MetaClusterCurrent.exist(MycatServer.class)) {
            mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);
            sum = mycatServer.countConnection();
        }
        GaugeMetricFamily gaugeMetricFamily =
                new GaugeMetricFamily("client_connection", "mycat session or connection", sum);
        return gaugeMetricFamily;
    }

    private static GaugeMetricFamily countCurrentProxyBackend() {
        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("native_mysql_connection", "mysql naivte backend connection",
                ImmutableList.of("type", "datasource"));

        if(MetaClusterCurrent.exist(MySQLManager.class)){
            MySQLManager mySQLManager = MetaClusterCurrent.wrapper(MySQLManager.class);
            Future<Map<String, Integer>> mapFuture = mySQLManager.computeConnectionUsageSnapshot();
            Map<String, Integer> integerMap = Collections.emptyMap();
            try {
                integerMap = mapFuture.toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
            }catch (Exception exception){
                LOGGER.error("",exception);
            }
            for (Map.Entry<String, Integer> entry : integerMap.entrySet()) {
                gaugeMetricFamily.addMetric(ImmutableList.of(Type.USE.getName(), entry.getKey()),
                        entry.getValue());
            }
        }
        return gaugeMetricFamily;
    }

    private static GaugeMetricFamily countCurrentJdbcBackend() {
        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("jdbc_connection",
                "jdbc backend connection",
                ImmutableList.of("type", "datasource"));

        if(MetaClusterCurrent.exist(JdbcConnectionManager.class)){
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            for (JdbcDataSource jdbcDataSource : jdbcConnectionManager.getDatasourceInfo().values()) {
                gaugeMetricFamily.addMetric(ImmutableList.of(Type.USE.getName(), jdbcDataSource.getName()),
                        jdbcDataSource.getUsedCount());
            }
        }
        return gaugeMetricFamily;
    }

    private static GaugeMetricFamily countTotalBackend() {
        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("instance_connection",
                "jdbc + native_mysql backend connection",
                ImmutableList.of("type", "datasource"));
        if(MetaClusterCurrent.exist(ReplicaSelectorManager.class)){
            ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            for (PhysicsInstance physicsInstance : replicaSelectorManager.getPhysicsInstances()) {
                gaugeMetricFamily.addMetric(ImmutableList.of(Type.TOTAL.getName(), physicsInstance.getName()), physicsInstance.getSessionCounter());
            }
        }
        return gaugeMetricFamily;
    }
}