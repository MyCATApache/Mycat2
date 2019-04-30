/**
 * Copyright (C) <2019>  <mycat>
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
package io.mycat.proxy.command;

/**
 * @author mycat
 * @date 2019-04-30 16:55
 **/
public class MySQLCommandType {
  // 前端报文类型
  /**
   * none, this is an internal thread state
   */
  public static final byte COM_SLEEP = 0;

  /**
   * mysql_close
   */
  public static final byte COM_QUIT = 1;

  public static final int COM_QUIT_PACKET_LENGTH = 1;

  /**
   * mysql_select_db
   */
  public static final byte COM_INIT_DB = 2;

  /**
   * mysql_real_query
   */
  public static final byte COM_QUERY = 3;

  /**
   * mysql_list_fields
   */
  public static final byte COM_FIELD_LIST = 4;

  /**
   * mysql_create_db (deprecated)
   */
  public static final byte COM_CREATE_DB = 5;

  /**
   * mysql_drop_db (deprecated)
   */
  public static final byte COM_DROP_DB = 6;

  /**
   * mysql_refresh
   */
  public static final byte COM_REFRESH = 7;

  /**
   * mysql_shutdown
   */
  public static final byte COM_SHUTDOWN = 8;

  /**
   * mysql_stat
   */
  public static final byte COM_STATISTICS = 9;

  /**
   * mysql_list_processes
   */
  public static final byte COM_PROCESS_INFO = 10;

  /**
   * none, this is an internal thread state
   */
  public static final byte COM_CONNECT = 11;

  /**
   * mysql_kill
   */
  public static final byte COM_PROCESS_KILL = 12;

  /**
   * mysql_dump_debug_info
   */
  public static final byte COM_DEBUG = 13;

  /**
   * mysql_ping
   */
  public static final byte COM_PING = 14;

  /**
   * none, this is an internal thread state
   */
  public static final byte COM_TIME = 15;

  /**
   * none, this is an internal thread state
   */
  public static final byte COM_DELAYED_INSERT = 16;

  /**
   * mysql_change_user
   */
  public static final byte COM_CHANGE_USER = 17;

  /**
   * used by slave server mysqlbinlog
   */
  public static final byte COM_BINLOG_DUMP = 18;

  /**
   * used by slave server to get master table
   */
  public static final byte COM_TABLE_DUMP = 19;

  /**
   * used by slave to log connection to master
   */
  public static final byte COM_CONNECT_OUT = 20;

  /**
   * used by slave to register to master
   */
  public static final byte COM_REGISTER_SLAVE = 21;

  /**
   * mysql_stmt_prepare
   */
  public static final byte COM_STMT_PREPARE = 22;

  /**
   * mysql_stmt_execute
   */
  public static final byte COM_STMT_EXECUTE = 23;

  /**
   * mysql_stmt_send_long_data
   */
  public static final byte COM_STMT_SEND_LONG_DATA = 24;

  /**
   * mysql_stmt_close
   */
  public static final byte COM_STMT_CLOSE = 25;

  /**
   * mysql_stmt_reset
   */
  public static final byte COM_STMT_RESET = 26;

  /**
   * mysql_set_server_option
   */
  public static final byte COM_SET_OPTION = 27;

  /**
   * mysql_stmt_fetch
   */
  public static final byte COM_STMT_FETCH = 28;

  /**
   * COM_DAEMON
   */
  public static final byte COM_DAEMON = 29;

  /**
   * COM_RESET_CONNECTION
   */
  public static final byte COM_RESET_CONNECTION = 31;

}
