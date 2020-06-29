package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.MycatCore;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.ReactorThreadManager;
import io.mycat.proxy.session.MySQLSessionManager;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConnectionCounterCollector extends Collector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCounterCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            return ImmutableList.of(
                    new CounterMetricFamily("client", "mycat session or connection",
                            countClient()),
                    countProxyBackend(),
                    countJdbcBackend()
            );
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }

    private long countClient() {
        return Optional.ofNullable(MycatCore.INSTANCE.getReactorManager())
                .map(ReactorThreadManager::getList)
                .orElse(Collections.emptyList())
                .stream().mapToLong(i -> i.getFrontManager().currentSessionCount()).sum();
    }

    private CounterMetricFamily countProxyBackend() {
        CounterMetricFamily counterMetricFamily = new CounterMetricFamily("native_mysql_connection", "mysql naivte backend connection",
                ImmutableList.of("reactor", "datasource"));
        for (MycatReactorThread i : Optional.ofNullable(MycatCore.INSTANCE.getReactorManager())
                .map(ReactorThreadManager::getList)
                .orElse(Collections.emptyList())) {
            MySQLSessionManager mySQLSessionManager = i.getMySQLSessionManager();
            Map<String, Long> collect = mySQLSessionManager.getAllSessions().stream().collect(Collectors.groupingBy(j -> j.getDatasourceName(), Collectors.counting()));
            for (Map.Entry<String, Long> e : collect.entrySet()) {
                counterMetricFamily.addMetric(ImmutableList.of(i.getName(), e.getKey()), e.getValue());
            }
        }
        return counterMetricFamily;
    }

    private CounterMetricFamily countJdbcBackend() {
        CounterMetricFamily counterMetricFamily = new CounterMetricFamily("jdbc_connection", "jdbc backend connection",
                ImmutableList.of( "datasource"));
        for (JdbcDataSource jdbcDataSource : Optional.ofNullable(JdbcRuntime.INSTANCE).map(i -> i.getConnectionManager()).map(i -> i.getDatasourceInfo())
                .map(i -> i.values()).orElse(Collections.emptyList())) {
            int usedCount = jdbcDataSource.getUsedCount();
            counterMetricFamily.addMetric(ImmutableList.of(jdbcDataSource.getName()),usedCount);
        }

        return counterMetricFamily;
    }
}