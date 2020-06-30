package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.MycatCore;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.ReactorThreadManager;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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
        long sum = Optional.ofNullable(MycatCore.INSTANCE.getReactorManager())
                .map(ReactorThreadManager::getList)
                .orElse(Collections.emptyList())
                .stream().mapToLong(i -> i.getFrontManager().currentSessionCount()).sum();
        GaugeMetricFamily gaugeMetricFamily =
                new GaugeMetricFamily("client_connection", "mycat session or connection", sum);
        return gaugeMetricFamily;
    }

    private static GaugeMetricFamily countCurrentProxyBackend() {
        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("native_mysql_connection", "mysql naivte backend connection",
                ImmutableList.of("type", "reactor", "datasource"));
        for (MycatReactorThread i : Optional.ofNullable(MycatCore.INSTANCE.getReactorManager())
                .map(ReactorThreadManager::getList)
                .orElse(Collections.emptyList())) {
            MySQLSessionManager mySQLSessionManager = i.getMySQLSessionManager();
            List<MySQLClientSession> allSessions = mySQLSessionManager.getAllSessions();
            Map<String, Long> totalMap = allSessions.stream().collect(Collectors.groupingBy(j -> j.getDatasourceName(), Collectors.counting()));
            totalMap.forEach((k, v) -> {
                gaugeMetricFamily.addMetric(ImmutableList.of(Type.TOTAL.getName(), k), v);
            });
            Map<String, Long>   idleMap = mySQLSessionManager.getAllSessions().stream().filter(j -> j.isIdle()).collect(Collectors.groupingBy(j -> j.getDatasourceName(), Collectors.counting()));
            idleMap.forEach((k, v) -> {
                gaugeMetricFamily.addMetric(ImmutableList.of(Type.IDLE.getName(), k), v);
            });
            Map<String, Long>   useMap = allSessions.stream().filter(j -> !j.isIdle()).collect(Collectors.groupingBy(j -> j.getDatasourceName(), Collectors.counting()));
            useMap.forEach((k, v) -> {
                gaugeMetricFamily.addMetric(ImmutableList.of(Type.USE.getName(), k), v);
            });
        }
        return gaugeMetricFamily;
    }

    private static GaugeMetricFamily countCurrentJdbcBackend() {
        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("jdbc_connection",
                "jdbc backend connection",
                ImmutableList.of("type", "datasource"));
        for (JdbcDataSource jdbcDataSource : Optional.ofNullable(JdbcRuntime.INSTANCE).map(i -> i.getConnectionManager()).map(i -> i.getDatasourceInfo())
                .map(i -> i.values()).orElse(Collections.emptyList())) {
            gaugeMetricFamily.addMetric(ImmutableList.of(Type.USE.getName(), jdbcDataSource.getName()),
                    jdbcDataSource.getUsedCount());
        }
        return gaugeMetricFamily;
    }

    private static GaugeMetricFamily countTotalBackend() {
        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("instance_connection",
                "jdbc + native_mysql backend connection",
                ImmutableList.of("type", "datasource"));

        Map<String, PhysicsInstance> physicsInstanceMap = Optional.ofNullable(ReplicaSelectorRuntime.INSTANCE.getPhysicsInstanceMap())
                .orElse(Collections.emptyMap());
        for (Map.Entry<String, PhysicsInstance> entry : physicsInstanceMap.entrySet()) {
            String key = entry.getKey();
            PhysicsInstance value = entry.getValue();
            gaugeMetricFamily.addMetric(ImmutableList.of(Type.TOTAL.getName(), key), value.getSessionCounter());
        }
        return gaugeMetricFamily;
    }
}