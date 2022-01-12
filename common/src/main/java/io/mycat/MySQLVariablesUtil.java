package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import io.mycat.beans.mycat.TransactionType;

import java.util.Objects;

public class MySQLVariablesUtil {
    public static void setVariable(MycatDataContext dataContext,
                                   String target,
                                   Object text) {
        String value = SQLUtils.normalize(Objects.toString(text));
        if (target.contains("autocommit")) {
            dataContext.setAutoCommit(toInt(value) == 1);
        } else if (target.equalsIgnoreCase("transaction_policy")) {
            if ("proxy".equalsIgnoreCase(value)) {
                dataContext.switchTransaction(TransactionType.PROXY_TRANSACTION_TYPE);
            } else if ("xa".equalsIgnoreCase(value)) {
                dataContext.switchTransaction(TransactionType.JDBC_TRANSACTION_TYPE);
            } else {
                throw new IllegalArgumentException("transaction_policy for " + value);
            }
        } else if (target.contains("net_write_timeout")) {
            dataContext.setVariable(MycatDataContextEnum.NET_WRITE_TIMEOUT, Long.parseLong(value));
        } else if ("SQL_SELECT_LIMIT".equalsIgnoreCase(target)) {
            if ("default".equalsIgnoreCase(value)) {
                value = Integer.toString(Integer.MAX_VALUE);
            }
            dataContext.setVariable(MycatDataContextEnum.SELECT_LIMIT, Long.parseLong(value));
        } else if ("character_set_results".equalsIgnoreCase(target)) {
            dataContext.setVariable(MycatDataContextEnum.CHARSET_SET_RESULT, value);
        } else if (target.contains("read_only")) {
            dataContext.setVariable(MycatDataContextEnum.IS_READ_ONLY, toInt(value));
        } else if (target.contains("transaction_isolation") || target.contains("tx_isolation")) {
            dataContext.setVariable(MycatDataContextEnum.IS_READ_ONLY, toInt(value));
        }
    }

    public static Object getVariable(MycatDataContext dataContext, String target) {
        target = target.toLowerCase();
        if (target.contains("autocommit")) {
            return dataContext.isAutocommit() ? 1 : 0;
        } else if (target.equalsIgnoreCase("transaction_policy")) {
            return dataContext.getTransactionSession().name();
        } else if (target.contains("net_write_timeout")) {
            return dataContext.getVariable(MycatDataContextEnum.NET_WRITE_TIMEOUT);
        } else if ("sql_select_limit".equalsIgnoreCase(target)) {
            return dataContext.getVariable(MycatDataContextEnum.SELECT_LIMIT);
        } else if ("character_set_results".equalsIgnoreCase(target)) {
            return dataContext.getVariable(MycatDataContextEnum.CHARSET_SET_RESULT);
        } else if (target.contains("read_only")) {
            return dataContext.getVariable(MycatDataContextEnum.IS_READ_ONLY);
        } else if (target.contains("current_user")) {
            return dataContext.getUser().getUserName();
        } else if (target.contains("transaction_isolation") || target.contains("tx_isolation")) {
            dataContext.getVariable(MycatDataContextEnum.ISOLATION);
        }
        //todo
//        Map<String, Object> map = RootHelper.INSTANCE.getConfigProvider().globalVariables();
//        MySQLVariablesEnum mySQLVariablesEnum = MySQLVariablesEnum.parseFromColumnName(target);
//        if (mySQLVariablesEnum != null) {
//            String columnName = mySQLVariablesEnum.getColumnName();
//            return map.getOrDefault(columnName, null);
//        } else {
//            return null;
//        }
        return null;
    }

    public static int toInt(Object s) {
        return toInt(Objects.toString(s));
    }

    public static int toInt(String s) {
        s = s.trim();
        if ("1".equalsIgnoreCase(s)) {
            return 1;
        }
        if ("0".equalsIgnoreCase(s)) {
            return 0;
        }
        if ("on".equalsIgnoreCase(s)) {
            return 1;
        }
        if ("off".equalsIgnoreCase(s)) {
            return 0;
        }
        if ("true".equalsIgnoreCase(s)) {
            return 1;
        }
        if ("false".equalsIgnoreCase(s)) {
            return 0;
        }
        return Integer.parseInt(s);
    }
}