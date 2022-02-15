package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.MetaClusterCurrent;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorManager;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InstanceCollector extends Collector {
    @Override
    public List<MetricFamilySamples> collect() {
        List<String> columnList = ImmutableList.of("NAME");
        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("instance_active",
                "instance_active",
                columnList);

        if (MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
            ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            for (PhysicsInstance physicsInstance : replicaSelectorManager.getPhysicsInstances()) {
                int value = physicsInstance.isAlive() ? 1 : 0;
                gaugeMetricFamily.addMetric(columnList, value);
            }
        }
        return ImmutableList.of(gaugeMetricFamily);
    }
}