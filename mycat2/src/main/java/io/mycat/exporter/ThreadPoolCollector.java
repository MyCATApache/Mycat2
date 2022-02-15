package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ThreadPoolCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            List<String> columnList = ImmutableList.of("ACTIVE_COUNT");
            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("thread_pool_active",
                    "thread_pool_active",
                    columnList);
            long count = 0;
            if(MetaClusterCurrent.exist(IOExecutor.class)){
               IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
                count = ioExecutor.count();
           }
            gaugeMetricFamily.addMetric(columnList, count);
            return ImmutableList.of(gaugeMetricFamily);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }
}