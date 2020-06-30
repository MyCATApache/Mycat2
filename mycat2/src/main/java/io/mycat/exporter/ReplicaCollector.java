package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.manager.commands.ShowReplicaCommand;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ReplicaCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            RowBaseIterator resultSet = ShowReplicaCommand.getResultSet().build();
            List<String> columnList = resultSet.getMetaData()
                    .getColumnList()
                    .stream()
                    .filter(i -> !"AVAILABLE".equalsIgnoreCase(i))
                    .collect(Collectors.toList());

            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("replica_available_value",
                    "replica_available_value",
                    columnList);

            List<Map<String, Object>> resultSetMap = resultSet.getResultSetMap();
            for (Map<String, Object> map : resultSetMap) {
                List<String> collect = columnList.stream()
                        .filter(i -> !"AVAILABLE".equalsIgnoreCase(i))
                        .map(s -> Objects.toString(map.get(s))).collect(Collectors.toList());
                Object available = map.get("AVAILABLE");
                int value = (Boolean.parseBoolean(available.toString())) ? 1 : 0;//check the value
                gaugeMetricFamily.addMetric(collect, value);
            }
            return ImmutableList.of(gaugeMetricFamily);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }
}