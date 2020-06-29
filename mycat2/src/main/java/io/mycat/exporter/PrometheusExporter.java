package io.mycat.exporter;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusExporter implements Exporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusExporter.class);

    @Override
    public void start() {
        try {
            DefaultExports.initialize();
            new ConnectionCounterCollector().register();
            HTTPServer server = new HTTPServer(7066);
        }catch (Throwable e){
            LOGGER.error("",e);
        }
    }

    public static void main(String[] args) {
        new PrometheusExporter().start();
    }
}