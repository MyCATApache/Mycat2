package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.MycatCore;
import io.mycat.buffer.BufferPool;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BufferPoolCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(BufferPoolCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("buffer_pool_counter",
                    "buffer_pool",
                    ImmutableList.of("name",  "chunkSize","capacity"));
            for (MycatReactorThread mycatReactorThread : Optional.ofNullable(MycatCore.INSTANCE.getReactorManager())
                    .map(i -> i.getList()).orElse(Collections.emptyList())) {
                BufferPool bufPool = mycatReactorThread.getBufPool();

                if (bufPool != null) {
                    String name = bufPool.getClass().getName();
                    long capacity = bufPool.capacity();
                    int chunkSize = bufPool.chunkSize();
                    int trace = bufPool.trace();
                    gaugeMetricFamily.addMetric(ImmutableList.of(String.valueOf(name),
                            String.valueOf(chunkSize),
                            String.valueOf(capacity)),
                            trace);
                }
            }

            return ImmutableList.of(gaugeMetricFamily);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }
}