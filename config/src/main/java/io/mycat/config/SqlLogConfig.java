package io.mycat.config;

import com.alibaba.druid.sql.parser.SQLType;
import lombok.Data;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Data
public class SqlLogConfig {
    boolean open = true;
    long sqlTimeFilter = TimeUnit.SECONDS.toMillis(30);
    Set<SQLType> sqlTypeFilter = new HashSet<>(Arrays.asList(
            SQLType.SELECT
    ));
    String clazz = "io.mycat.exporter.MySQLLogConsumer";
}
