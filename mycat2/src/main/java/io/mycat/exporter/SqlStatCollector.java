package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.monitor.SqlEntry;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;

public class SqlStatCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlStatCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("sql_stat",
                    "sql_stat",
                    ImmutableList.of("statement", "time_type"));

            List<SqlEntry> records = SqlRecorderRuntime.INSTANCE.getRecords();
            for (SqlEntry record : records) {
                if (record.getSqlTime() > 0) {

                    String sql = "/*+ " + record.getTraceId() + "*/" + record.getSql().toString();
                    gaugeMetricFamily.addMetric(ImmutableList.of(sql, "EXECUTE_TIME"), record.getSqlTime());
                    gaugeMetricFamily.addMetric(ImmutableList.of(sql, "START_TIME"),
                            record.getResponseTime().minus(Duration.ofMillis(record.getSqlTime())).toEpochSecond(ZoneOffset.UTC));
                    gaugeMetricFamily.addMetric(ImmutableList.of(sql, "END_TIME"), record.getResponseTime().toEpochSecond(ZoneOffset.UTC));
                }
            }
            return ImmutableList.of(gaugeMetricFamily);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }
}