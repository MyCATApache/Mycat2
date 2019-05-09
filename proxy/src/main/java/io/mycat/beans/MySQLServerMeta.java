/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans;

import io.mycat.beans.mysql.MySQLCapabilities;
import io.mycat.beans.mysql.MySQLServerCapabilityFlags;

public class MySQLServerMeta {
    public MySQLMetaType getType() {
        return type;
    }

    MySQLMetaType type = MySQLMetaType.SLAVE_NODE;
    private static MySQLServerCapabilityFlags capabilityFlags = new MySQLServerCapabilityFlags(initClientFlags());

    public static MySQLServerCapabilityFlags getClientCapabilityFlags() {
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
