package io.mycat.exporter;

import io.mycat.MycatConfig;
import io.mycat.RootHelper;
import io.prometheus.client.hotspot.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class PrometheusExporter implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusExporter.class);

    @Override
    public void run() {
        Optional.ofNullable(RootHelper.INSTANCE.getConfigProvider())
                .map(i->i.currentConfig())
                .map(i->i.getProperties())
                .map(i->(Integer)i.getOrDefault("prometheusPort",7066))
                .ifPresent(port->{
                    try {
                        CollectorList collectorList = new CollectorList(
                                new StandardExports(),
                                new MemoryPoolsExports(),
                                new BufferPoolsExports(),
                                new GarbageCollectorExports(),
                                new ThreadExports(),
                                new ClassLoadingExports(),
                                new VersionInfoExports(),
                                //////////////////////////////////////////
                                new ConnectionCounterCollector(),
                                new SqlStatCollector(),
                                new BufferPoolCollector(),
                                new HeartbeatCollector(),
                                new ReplicaCollector(),
                                new ThreadPoolCollector(),
                                new InstanceCollector(),
                                new CPULoadCollector()
                        );
                        collectorList.register();
                        HTTPServer server = new io.mycat.exporter.HTTPServer(port);
                    } catch (Throwable e) {
                        LOGGER.error("", e);
                    }
                });
    }
}