package io.mycat.exporter;

import io.prometheus.client.hotspot.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusExporter implements Exporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusExporter.class);

    @Override
    public void start() {
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
            HTTPServer server = new io.mycat.exporter.HTTPServer(7066);
        } catch (Throwable e) {
            LOGGER.error("", e);
        }
    }

    public static void main(String[] args) {
        new PrometheusExporter().start();
    }
}