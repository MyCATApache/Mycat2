package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.manager.commands.ShowReplicaCommand;
import io.mycat.manager.commands.ShowThreadPoolCommand;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ThreadPoolCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            RowBaseIterator resultSet = ShowThreadPoolCommand.getResultSet().build();
            List<String> columnList = resultSet.getMetaData().getColumnList()
                    .stream()
                    .filter(i -> !"ACTIVE_COUNT".equalsIgnoreCase(i))
                    .collect(Collectors.toList());
            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("thread_pool_active",
                    "thread_pool_active",
                    columnList);

            List<Map<String, Object>> resultSetMap = resultSet.getResultSetMap();
            for (Map<String, Object> map : resultSetMap) {
                List<String> collect = columnList
                        .stream()
                        .filter(i -> !"ACTIVE_COUNT".equalsIgnoreCase(i))
                        .map(s -> Objects.toString(map.get(s)))
                        .collect(Collectors.toList());
                Number ACTIVE_COUNT = (Number) map.get("ACTIVE_COUNT");
                gaugeMetricFamily.addMetric(collect, ACTIVE_COUNT.doubleValue());
            }
            return ImmutableList.of(gaugeMetricFamily);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }
}