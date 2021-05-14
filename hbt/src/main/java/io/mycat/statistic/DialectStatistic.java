package io.mycat.statistic;

import io.mycat.statistic.histogram.MySQLHistogram;

import java.util.Optional;

public interface DialectStatistic {
    double rowCount(String targetName,String schemaName,String tableName);
    Optional<MySQLHistogram> histogram(String targetName, String schemaName, String tableName, String columnName);
}
