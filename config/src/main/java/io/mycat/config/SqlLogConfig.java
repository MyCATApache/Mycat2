package io.mycat.config;

import com.alibaba.druid.sql.parser.SQLType;
import io.mycat.util.SqlTypeUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SqlLogConfig {
    boolean open = true;
    long sqlTimeFilter = TimeUnit.SECONDS.toMillis(1) / 2;
    Set<SQLType> sqlTypeFilter = new HashSet<>(Arrays.asList(
            SQLType.SELECT,SQLType.INSERT,SQLType.INSERT_VALUES,SQLType.UPDATE,SQLType.DELETE
    ));
    String clazz;
}
