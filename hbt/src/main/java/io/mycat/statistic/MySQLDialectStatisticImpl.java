package io.mycat.statistic;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.builder.SQLBuilderFactory;
import com.alibaba.druid.sql.builder.SQLSelectBuilder;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.DataNode;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.statistic.histogram.MySQLHistogram;
import io.mycat.statistic.histogram.MySQLHistogramParser;
import io.mycat.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

public class MySQLDialectStatisticImpl implements DialectStatistic {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDialectStatisticImpl.class);

    @Override
    public double rowCount(String targetName, String schemaName, String tableName) {
        String sql = makeCountSql(schemaName, tableName);
        try {
            ReplicaSelectorManager runtime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            targetName = runtime.getDatasourceNameByReplicaName(targetName, false, null);
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
                try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql)) {
                    rowBaseIterator.next();
                    return rowBaseIterator.getBigDecimal(1).doubleValue();
                }
            }
        } catch (Throwable e) {
            LOGGER.error("不能获取行统计 " + targetName + " " + sql, e);
        }
        return 500_000;
    }

    private String makeCountSql(String schemaName, String tableName) {
        SQLSelectBuilder selectSQLBuilder = SQLBuilderFactory.createSelectSQLBuilder(DbType.mysql);
        return selectSQLBuilder.from(schemaName + "." + tableName).select("count(1)").toString();
    }


    @Override
    public Optional<MySQLHistogram> histogram(String targetName, String schemaName, String tableName, String columnName) {
        try {
            ReplicaSelectorManager runtime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            targetName = runtime.getDatasourceNameByReplicaName(targetName, false, null);
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
                List<Map<String, Object>> maps = JdbcUtils.executeQuery(connection.getRawConnection(),
                        "SELECT * FROM `information_schema`.`COLUMN_STATISTICS` WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?; ",
                        Arrays.asList(schemaName, tableName, columnName));
                String histogram = Objects.toString(maps.get(0).get("HISTOGRAM"));
                return Optional.of(MySQLHistogramParser.parse(histogram));
            }
        } catch (Throwable e) {
            LOGGER.warn("", e);
            return Optional.empty();
        }
    }
}
