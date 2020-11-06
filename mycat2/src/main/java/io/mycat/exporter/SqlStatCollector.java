//package io.mycat.exporter;
//
//import com.google.common.collect.ImmutableList;
//import io.mycat.sqlrecorder.SqlRecord;
//import io.mycat.sqlrecorder.SqlRecorderRuntime;
//import io.prometheus.client.Collector;
//import io.prometheus.client.GaugeMetricFamily;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.List;
//import java.util.Map;
//
//public class SqlStatCollector extends Collector {
//    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusExporter.class);
//
//    @Override
//    public List<MetricFamilySamples> collect() {
//        try {
//            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("sql_stat",
//                    "sql_stat",
//                    ImmutableList.of("statement", "time_type"));
//            Map<String, List<SqlRecord>> recordList = SqlRecorderRuntime.INSTANCE.getRecordList();
//            for (Map.Entry<String, List<SqlRecord>> entry : recordList.entrySet()) {
//                String sql = normalie(entry.getKey());
//                for (SqlRecord record : entry.getValue()) {
//                    if (record.getStatement() != null) {
//                        double v = record.getEndTime() - record.getStartTime();
//                        if (v > 0) {
//                            gaugeMetricFamily.addMetric(ImmutableList.of(sql, "TOTAL_TIME"), v);
//                            gaugeMetricFamily.addMetric(ImmutableList.of(sql, "COMPILE_TIME"), record.getCompileTime());
//                            gaugeMetricFamily.addMetric(ImmutableList.of(sql, "RBO_TIME"), record.getCboTime());
//                            gaugeMetricFamily.addMetric(ImmutableList.of(sql, "CBO_TIME"), record.getRboTime());
//                            gaugeMetricFamily.addMetric(ImmutableList.of(sql, "CONNECTION_POOL_TIME"), record.getConnectionPoolTime());
//                            gaugeMetricFamily.addMetric(ImmutableList.of(sql, "CONNECTION_QUERY_TIME"), record.getConnectionQueryTime());
//                            gaugeMetricFamily.addMetric(ImmutableList.of(sql, "EXECUTION_TIME"), record.getExecutionTime());
//                        }
//                    }
//                }
//            }
//            return ImmutableList.of(gaugeMetricFamily);
//        } catch (Throwable e) {
//            LOGGER.error("", e);
//            throw e;
//        }
//    }
//
//
//    private String normalie(String key) {
//        int length = key.length();
//        StringBuilder builder = new StringBuilder();
//        char lastChar = 0;
//        for (int i = 0; i < length; i++) {
//            char a = key.charAt(i);
//            if (Character.isLetterOrDigit(a)) {
//                builder.append(lastChar = a);
//            } else if (builder.length() > 0&& lastChar != '_') {
//                builder.append(lastChar = '_');
//            }
//        }
//        String s = builder.toString();
//        if (s.endsWith("_")){
//            return s.substring(0,s.length()-1);
//        }else {
//            return s;
//        }
//    }
//}