package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.sqlhandler.dql.HintHandler;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ReplicaCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            RowBaseIterator rowBaseIterator = HintHandler.showClusters(null);
            List<String> columns = Arrays.asList("NAME", "SWITCH_TYPE", "WRITE_DS", "READ_DS", "AVAILABLE");
            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("replica_available_value",
                    "replica_available_value", columns);

            List<Map<String, Object>> resultSetMap = rowBaseIterator.getResultSetMap();
            for (Map<String, Object> map : resultSetMap) {
                List<String> collect = map.keySet().stream()
                        .filter(i -> columns.contains(i))
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