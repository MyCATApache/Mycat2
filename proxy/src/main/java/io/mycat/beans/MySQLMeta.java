package io.mycat.beans;

import io.mycat.beans.mysql.MySQLCapabilities;
import io.mycat.beans.mysql.MySQLCapabilityFlags;

public class MySQLMeta {
    public MySQLMetaType getType() {
        return type;
    }

    MySQLMetaType type = MySQLMetaType.SLAVE_NODE;
    private static MySQLCapabilityFlags capabilityFlags = new MySQLCapabilityFlags(initClientFlags());

    public static MySQLCapabilityFlags getClientCapabilityFlags() {
        return capabilityFlags;
    }

    private static int initClientFlags() {
        int flag = 0;
        flag |= MySQLCapabilities.CLIENT_LONG_PASSWORD;
        flag |= MySQLCapabilities.CLIENT_FOUND_ROWS;
        flag |= MySQLCapabilities.CLIENT_LONG_FLAG;
        // flag |= MySQLCapabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= MySQLCapabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = false;
        if (usingCompress) {
            flag |= MySQLCapabilities.CLIENT_COMPRESS;
        }
        flag |= MySQLCapabilities.CLIENT_ODBC;
        flag |= MySQLCapabilities.CLIENT_LOCAL_FILES;
        flag |= MySQLCapabilities.CLIENT_IGNORE_SPACE;
        flag |= MySQLCapabilities.CLIENT_PROTOCOL_41;
        flag |= MySQLCapabilities.CLIENT_INTERACTIVE;
        // flag |= MySQLCapabilities.CLIENT_SSL;
        flag |= MySQLCapabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= MySQLCapabilities.CLIENT_TRANSACTIONS;
        // flag |= MySQLCapabilities.CLIENT_RESERVED;
        flag |= MySQLCapabilities.CLIENT_SECURE_CONNECTION;
        flag |= MySQLCapabilities.CLIENT_PLUGIN_AUTH;

        flag |= MySQLCapabilities.CLIENT_MULTI_STATEMENTS;
        flag |= MySQLCapabilities.CLIENT_MULTI_RESULTS;
        // // client extension
        // flag |= MySQLCapabilities.CLIENT_MULTI_STATEMENTS;
        // flag |= MySQLCapabilities.CLIENT_MULTI_RESULTS;
        return flag;
    }

}
