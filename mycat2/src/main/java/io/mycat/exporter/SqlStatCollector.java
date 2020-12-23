package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SqlStatCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlStatCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("sql_stat",
                    "sql_stat",
                    ImmutableList.of("statement", "time_type"));
            List<SqlRecord> records = SqlRecorderRuntime.INSTANCE.getRecords();
            for (SqlRecord record : records) {
                double v = record.getEndTime() - record.getStartTime();
                if (record.getExecuteTime() > 0) {

                    String sql = "/*+ " + record.getId() + "/" + record.getTarget() + "*/" + record.getSql().toString();
                    gaugeMetricFamily.addMetric(ImmutableList.of(sql, "EEXECUTE_TIME"), record.getExecuteTime());
                    gaugeMetricFamily.addMetric(ImmutableList.of(sql, "START_TIME"), record.getStartTime());
                    gaugeMetricFamily.addMetric(ImmutableList.of(sql, "END_TIME"), record.getEndTime());
                }
            }
            return ImmutableList.of(gaugeMetricFamily);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }
}