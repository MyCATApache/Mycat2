/**
 * Copyright (C) <2019>  <gaozhiwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.config.MySQLServerCapabilityFlags;

/**
 * Desc:
 *
 * date: 24/09/2017,10/4/2019
 * @author: gaozhiwen
 */
public class GlobalConst {


    public static final String SINGLE_NODE_HEARTBEAT_SQL = "select 1";
    public static final String MASTER_SLAVE_HEARTBEAT_SQL = "show slave status";
    public static final String GARELA_CLUSTER_HEARTBEAT_SQL = "show status like 'wsrep%'";
    public static final String GROUP_REPLICATION_HEARTBEAT_SQL = "show slave status";

    public static final String[] MYSQL_SLAVE_STAUTS_COLMS = {
            "Seconds_Behind_Master",
            "Slave_IO_Running",
            "Slave_SQL_Running",
            "Slave_IO_State",
            "Master_Host",
            "Master_User",
            "Master_Port",
            "Connect_Retry",
            "Last_IO_Error"};

    public static final String[] MYSQL_CLUSTER_STAUTS_COLMS = {"Variable_name", "Value"};


    private static MySQLServerCapabilityFlags capabilityFlags = new MySQLServerCapabilityFlags(initClientFlags());

    public static MySQLServerCapabilityFlags getClientCapabilityFlags() {
        return capabilityFlags;
    }

    /**
     * 向后端mysql创建连接的服务器能力
     */
    private static int initClientFlags() {
        int flag = 0;
        flag |= MySQLServerCapabilityFlags.CLIENT_LONG_PASSWORD;
        flag |= MySQLServerCapabilityFlags.CLIENT_FOUND_ROWS;
        flag |= MySQLServerCapabilityFlags.CLIENT_LONG_FLAG;
        // flag |= MySQLServerCapabilityFlags.CLIENT_CONNECT_WITH_DB;
        // flag |= MySQLServerCapabilityFlags.CLIENT_NO_SCHEMA;
        boolean usingCompress = false;
        if (usingCompress) {
            flag |= MySQLServerCapabilityFlags.CLIENT_COMPRESS;
        }
        flag |= MySQLServerCapabilityFlags.CLIENT_ODBC;
        flag |= MySQLServerCapabilityFlags.CLIENT_LOCAL_FILES;
        flag |= MySQLServerCapabilityFlags.CLIENT_IGNORE_SPACE;
        flag |= MySQLServerCapabilityFlags.CLIENT_PROTOCOL_41;
        flag |= MySQLServerCapabilityFlags.CLIENT_INTERACTIVE;
        // flag |= MySQLServerCapabilityFlags.CLIENT_SSL;
        flag |= MySQLServerCapabilityFlags.CLIENT_IGNORE_SIGPIPE;
        flag |= MySQLServerCapabilityFlags.CLIENT_TRANSACTIONS;
        // flag |= MySQLServerCapabilityFlags.CLIENT_RESERVED;
        flag |= MySQLServerCapabilityFlags.CLIENT_SECURE_CONNECTION;
        flag |= MySQLServerCapabilityFlags.CLIENT_PLUGIN_AUTH;

        flag |= MySQLServerCapabilityFlags.CLIENT_MULTI_STATEMENTS;
        flag |= MySQLServerCapabilityFlags.CLIENT_MULTI_RESULTS;
        // // client extension
        // flag |= MySQLServerCapabilityFlags.CLIENT_MULTI_STATEMENTS;
        // flag |= MySQLServerCapabilityFlags.CLIENT_MULTI_RESULTS;
        return flag;
    }
}
