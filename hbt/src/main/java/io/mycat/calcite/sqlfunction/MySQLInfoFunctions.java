package io.mycat.calcite.sqlfunction;

import io.mycat.MycatUser;
import io.mycat.hbt4.MycatContext;

public class MySQLInfoFunctions {

    public static long CONNECTION_ID() {
        return MycatContext.CONTEXT.get().getSessionId();
    }

    public static long LAST_INSERT_ID() {
        return MycatContext.CONTEXT.get().getLastInsertId();
    }

    public static long LAST_INSERT_ID(Long expr) {
        return expr;
    }

    public static String CURRENT_USER() {
        MycatUser user = MycatContext.CONTEXT.get().getUser();
        return user.getHost()+"@"+user.getUserName();
    }

    public static String DATABASE() {
        String defaultSchema = MycatContext.CONTEXT.get().getDefaultSchema();
        return defaultSchema;
    }

    public static String SCHEMA() {
        return DATABASE();
    }

    public static String SESSION_USER() {
        return CURRENT_USER();
    }

    public static String SYSTEM_USER() {
        return CURRENT_USER();
    }

    public static String USER() {
        return CURRENT_USER();
    }

    public static String VERSION() {
        return "8.0.19";
    }
}