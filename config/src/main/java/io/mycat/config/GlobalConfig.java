/**
 * Copyright (C) <2019>  <gaozhiwen>
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
package io.mycat.config;

/**
 * Desc:
 *
 * @date: 24/09/2017,10/4/2019
 * @author: gaozhiwen
 */
public class GlobalConfig {

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

    public static final int INIT_VERSION = 1;
    // 默认的重试次数
    public static final int MAX_RETRY_COUNT = 5;
}
