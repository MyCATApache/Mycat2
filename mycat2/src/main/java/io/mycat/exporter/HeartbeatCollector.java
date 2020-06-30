package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.mycat.MycatCore;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.buffer.BufferPool;
import io.mycat.manager.commands.ShowHeartbeatCommand;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class HeartbeatCollector  extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        try {
            ResultSetBuilder resultSet = ShowHeartbeatCommand.getResultSet();
            RowBaseIterator rowBaseIterator = resultSet.build();
            MycatRowMetaData metaData = rowBaseIterator.getMetaData();
            List<String> columnList = metaData.getColumnList();
            List<Map<String, Object>> resultSetMap = rowBaseIterator.getResultSetMap();
            GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("heartbeat_stat",
                    "heartbeat_stat",
                    columnList);
            for (Map<String, Object> stringObjectMap : resultSetMap) {
                Date LAST_SEND_QUERY_TIME= (Date)stringObjectMap.get("LAST_SEND_QUERY_TIME");
                Date LAST_RECEIVED_QUERY_TIME = (Date)stringObjectMap.get("LAST_RECEIVED_QUERY_TIME");
                //查询时间之差
                long l = LAST_RECEIVED_QUERY_TIME.getTime() - LAST_SEND_QUERY_TIME.getTime();
                List<String> collect = columnList.stream().map(i -> stringObjectMap.get(i))
                        .map(i -> Objects.toString(i)).collect(Collectors.toList());

                gaugeMetricFamily.addMetric(
                        collect,
                        l);
            }
            return ImmutableList.of(gaugeMetricFamily);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }
}