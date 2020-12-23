package io.mycat.exporter;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.MycatServerConfig;
import io.prometheus.client.hotspot.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

public class PrometheusExporter implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusExporter.class);

    @Override
    public void run() {
        MycatServerConfig mycatServerConfig = MetaClusterCurrent.wrapper(MycatServerConfig.class);
        Optional.ofNullable(mycatServerConfig.getProperties().getOrDefault("prometheusPort",7066))
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
                                new SqlStatCollector(),
                                new ReplicaCollector(),
                                new CPULoadCollector()
                        );
                        collectorList.register();
                        HTTPServer server = new io.mycat.exporter.HTTPServer(Integer.parseInt(Objects.toString(port)));
                    } catch (Throwable e) {
                        LOGGER.error("", e);
                    }
                });
    }
}