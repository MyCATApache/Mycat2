package io.mycat.exporter;

import io.prometheus.client.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CollectorList extends Collector {

    final List<Collector> collectors;

    public CollectorList(Collector... collectors) {
        this.collectors = Arrays.asList(collectors);
    }

    public CollectorList(List<Collector> collectors) {
        this.collectors = collectors;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return collectors.stream().flatMap(i -> i.collect().stream()).collect(Collectors.toList());
    }
}