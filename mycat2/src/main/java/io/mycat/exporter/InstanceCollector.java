//package io.mycat.exporter;
//
//import com.google.common.collect.ImmutableList;
//import io.mycat.api.collector.RowBaseIterator;
//import io.prometheus.client.Collector;
//import io.prometheus.client.GaugeMetricFamily;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class InstanceCollector extends Collector {
//    @Override
//    public List<MetricFamilySamples> collect() {
//        RowBaseIterator rowBaseIterator = ShowInstanceCommand.getResultSet().build();
//        List<String> columnList =ImmutableList.of("NAME");
//        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("instance_acitve",
//                "instance_acitve",
//                columnList);
//        List<Map<String, Object>> resultSetMap = rowBaseIterator.getResultSetMap();
//        for (Map<String, Object> stringObjectMap : resultSetMap) {
//            List<String> collect = columnList.stream().map(s -> Objects.toString(stringObjectMap.get(s))).collect(Collectors.toList());
//            Object alive = stringObjectMap.get("ALIVE");
//            int value = (alive==Boolean.TRUE||"1".equalsIgnoreCase(Objects.toString(alive)))?1:0;
//            gaugeMetricFamily.addMetric(collect,value);
//        }
//
//        return ImmutableList.of(gaugeMetricFamily);
//    }
//}