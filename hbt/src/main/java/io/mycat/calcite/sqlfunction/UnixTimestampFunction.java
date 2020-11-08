package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;
import org.apache.calcite.linq4j.function.Parameter;

public  class UnixTimestampFunction {
        public static Long eval(@Parameter(name = "date") String dateText) {
            return ((Number) UnsolvedMysqlFunctionUtil.eval("UNIX_TIMESTAMP", dateText)).longValue();
        }
    }