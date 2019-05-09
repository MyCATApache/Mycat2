/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql;

/**
 * @author jamie12221
 * @date 2019-05-07 17:10
 **/
public interface MySQLSQLCommandType {

  public static void main(String[] args) {
    int sqlType = 0;
    switch (sqlType) {
      case MySQLSQLCommandType.SQLCOM_SELECT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_TABLE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_INDEX: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_TABLE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_UPDATE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_INSERT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_INSERT_SELECT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DELETE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_TRUNCATE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_TABLE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_DB: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_INDEX: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_DATABASES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_TABLES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_FIELDS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_KEYS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_VARIABLES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_STATUS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_ENGINE_LOGS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_ENGINE_STATUS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_ENGINE_MUTEX: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_PROCESSLIST: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_MASTER_STAT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_SLAVE_STAT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_GRANTS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CREATE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CHARSETS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_COLLATIONS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CREATE_DB: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_TABLE_STATUS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_TRIGGERS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_LOAD: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SET_OPTION: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_LOCK_TABLES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_UNLOCK_TABLES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_GRANT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CHANGE_DB: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_DB: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_DB: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_REPAIR: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_REPLACE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_REPLACE_SELECT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_FUNCTION: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_FUNCTION: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_REVOKE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_OPTIMIZE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CHECK: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ASSIGN_TO_KEYCACHE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_PRELOAD_KEYS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_FLUSH: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_KILL: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ANALYZE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ROLLBACK: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ROLLBACK_TO_SAVEPOINT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_COMMIT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SAVEPOINT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_RELEASE_SAVEPOINT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SLAVE_START: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SLAVE_STOP: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_START_GROUP_REPLICATION: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_STOP_GROUP_REPLICATION: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_BEGIN: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CHANGE_MASTER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CHANGE_REPLICATION_FILTER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_RENAME_TABLE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_RESET: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_PURGE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_PURGE_BEFORE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_BINLOGS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_OPEN_TABLES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_HA_OPEN: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_HA_CLOSE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_HA_READ: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_SLAVE_HOSTS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DELETE_MULTI: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_UPDATE_MULTI: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_BINLOG_EVENTS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DO: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_WARNS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_EMPTY_QUERY: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_ERRORS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_STORAGE_ENGINES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_PRIVILEGES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_HELP: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_USER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_USER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_RENAME_USER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_REVOKE_ALL: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CHECKSUM: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_PROCEDURE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_SPFUNCTION: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CALL: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_PROCEDURE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_PROCEDURE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_FUNCTION: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CREATE_PROC: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CREATE_FUNC: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_STATUS_PROC: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_STATUS_FUNC: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_PREPARE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_EXECUTE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DEALLOCATE_PREPARE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_VIEW: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_VIEW: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_TRIGGER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_TRIGGER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_XA_START: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_XA_END: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_XA_COMMIT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_XA_ROLLBACK: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_XA_RECOVER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_PROC_CODE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_FUNC_CODE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_TABLESPACE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_INSTALL_PLUGIN: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_UNINSTALL_PLUGIN: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_BINLOG_BASE64_EVENT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_PLUGINS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_SERVER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_SERVER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_SERVER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_CREATE_EVENT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_EVENT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_DROP_EVENT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CREATE_EVENT: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_EVENTS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CREATE_TRIGGER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_DB_UPGRADE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_PROFILE: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_PROFILES: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SIGNAL: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_RESIGNAL: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_RELAYLOG_EVENTS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_GET_DIAGNOSTICS: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_USER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_EXPLAIN_OTHER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHOW_CREATE_USER: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_SHUTDOWN: {
        break;
      }
      case MySQLSQLCommandType.SQLCOM_ALTER_INSTANCE: {
        break;
      }


    }
  }

  final int SQLCOM_SELECT = 3;
  final int SQLCOM_CREATE_TABLE = 4;
  final int SQLCOM_CREATE_INDEX = 5;
  final int SQLCOM_ALTER_TABLE = 6;
  final int SQLCOM_UPDATE = 7;
  final int SQLCOM_INSERT = 8;
  final int SQLCOM_INSERT_SELECT = 9;
  final int SQLCOM_DELETE = 10;
  final int SQLCOM_TRUNCATE = 11;
  final int SQLCOM_DROP_TABLE = 12;
  final int SQLCOM_DROP_INDEX = 13;
  final int SQLCOM_SHOW_DATABASES = 14;
  final int SQLCOM_SHOW_TABLES = 15;
  final int SQLCOM_SHOW_FIELDS = 16;
  final int SQLCOM_SHOW_KEYS = 17;
  final int SQLCOM_SHOW_VARIABLES = 18;
  final int SQLCOM_SHOW_STATUS = 19;
  final int SQLCOM_SHOW_ENGINE_LOGS = 20;
  final int SQLCOM_SHOW_ENGINE_STATUS = 21;
  final int SQLCOM_SHOW_ENGINE_MUTEX = 22;
  final int SQLCOM_SHOW_PROCESSLIST = 23;
  final int SQLCOM_SHOW_MASTER_STAT = 24;
  final int SQLCOM_SHOW_SLAVE_STAT = 25;
  final int SQLCOM_SHOW_GRANTS = 26;
  final int SQLCOM_SHOW_CREATE = 27;
  final int SQLCOM_SHOW_CHARSETS = 28;
  final int SQLCOM_SHOW_COLLATIONS = 29;
  final int SQLCOM_SHOW_CREATE_DB = 30;
  final int SQLCOM_SHOW_TABLE_STATUS = 31;
  final int SQLCOM_SHOW_TRIGGERS = 32;
  final int SQLCOM_LOAD = 33;
  final int SQLCOM_SET_OPTION = 34;
  final int SQLCOM_LOCK_TABLES = 35;
  final int SQLCOM_UNLOCK_TABLES = 36;
  final int SQLCOM_GRANT = 37;
  final int SQLCOM_CHANGE_DB = 38;
  final int SQLCOM_CREATE_DB = 39;
  final int SQLCOM_DROP_DB = 40;
  final int SQLCOM_ALTER_DB = 41;
  final int SQLCOM_REPAIR = 42;
  final int SQLCOM_REPLACE = 43;
  final int SQLCOM_REPLACE_SELECT = 44;
  final int SQLCOM_CREATE_FUNCTION = 45;
  final int SQLCOM_DROP_FUNCTION = 46;
  final int SQLCOM_REVOKE = 47;
  final int SQLCOM_OPTIMIZE = 48;
  final int SQLCOM_CHECK = 49;
  final int SQLCOM_ASSIGN_TO_KEYCACHE = 50;
  final int SQLCOM_PRELOAD_KEYS = 51;
  final int SQLCOM_FLUSH = 52;
  final int SQLCOM_KILL = 53;
  final int SQLCOM_ANALYZE = 54;
  final int SQLCOM_ROLLBACK = 55;
  final int SQLCOM_ROLLBACK_TO_SAVEPOINT = 56;
  final int SQLCOM_COMMIT = 57;
  final int SQLCOM_SAVEPOINT = 58;
  final int SQLCOM_RELEASE_SAVEPOINT = 59;
  final int SQLCOM_SLAVE_START = 60;
  final int SQLCOM_SLAVE_STOP = 61;
  final int SQLCOM_START_GROUP_REPLICATION = 62;
  final int SQLCOM_STOP_GROUP_REPLICATION = 63;
  final int SQLCOM_BEGIN = 64;
  final int SQLCOM_CHANGE_MASTER = 65;
  final int SQLCOM_CHANGE_REPLICATION_FILTER = 66;
  final int SQLCOM_RENAME_TABLE = 67;
  final int SQLCOM_RESET = 68;
  final int SQLCOM_PURGE = 69;
  final int SQLCOM_PURGE_BEFORE = 70;
  final int SQLCOM_SHOW_BINLOGS = 71;
  final int SQLCOM_SHOW_OPEN_TABLES = 72;
  final int SQLCOM_HA_OPEN = 73;
  final int SQLCOM_HA_CLOSE = 74;
  final int SQLCOM_HA_READ = 75;
  final int SQLCOM_SHOW_SLAVE_HOSTS = 76;
  final int SQLCOM_DELETE_MULTI = 77;
  final int SQLCOM_UPDATE_MULTI = 78;
  final int SQLCOM_SHOW_BINLOG_EVENTS = 79;
  final int SQLCOM_DO = 80;
  final int SQLCOM_SHOW_WARNS = 81;
  final int SQLCOM_EMPTY_QUERY = 82;
  final int SQLCOM_SHOW_ERRORS = 83;
  final int SQLCOM_SHOW_STORAGE_ENGINES = 84;
  final int SQLCOM_SHOW_PRIVILEGES = 85;
  final int SQLCOM_HELP = 86;
  final int SQLCOM_CREATE_USER = 87;
  final int SQLCOM_DROP_USER = 88;
  final int SQLCOM_RENAME_USER = 89;
  final int SQLCOM_REVOKE_ALL = 90;
  final int SQLCOM_CHECKSUM = 91;
  final int SQLCOM_CREATE_PROCEDURE = 92;
  final int SQLCOM_CREATE_SPFUNCTION = 93;
  final int SQLCOM_CALL = 94;
  final int SQLCOM_DROP_PROCEDURE = 95;
  final int SQLCOM_ALTER_PROCEDURE = 96;
  final int SQLCOM_ALTER_FUNCTION = 97;
  final int SQLCOM_SHOW_CREATE_PROC = 98;
  final int SQLCOM_SHOW_CREATE_FUNC = 99;
  final int SQLCOM_SHOW_STATUS_PROC = 100;
  final int SQLCOM_SHOW_STATUS_FUNC = 101;
  final int SQLCOM_PREPARE = 102;
  final int SQLCOM_EXECUTE = 103;
  final int SQLCOM_DEALLOCATE_PREPARE = 104;
  final int SQLCOM_CREATE_VIEW = 105;
  final int SQLCOM_DROP_VIEW = 106;
  final int SQLCOM_CREATE_TRIGGER = 107;
  final int SQLCOM_DROP_TRIGGER = 108;
  final int SQLCOM_XA_START = 109;
  final int SQLCOM_XA_END = 110;
  final int SQLCOM_XA_COMMIT = 111;
  final int SQLCOM_XA_ROLLBACK = 112;
  final int SQLCOM_XA_RECOVER = 113;
  final int SQLCOM_SHOW_PROC_CODE = 114;
  final int SQLCOM_SHOW_FUNC_CODE = 115;
  final int SQLCOM_ALTER_TABLESPACE = 116;
  final int SQLCOM_INSTALL_PLUGIN = 117;
  final int SQLCOM_UNINSTALL_PLUGIN = 118;
  final int SQLCOM_BINLOG_BASE64_EVENT = 119;
  final int SQLCOM_SHOW_PLUGINS = 120;
  final int SQLCOM_CREATE_SERVER = 121;
  final int SQLCOM_DROP_SERVER = 122;
  final int SQLCOM_ALTER_SERVER = 123;
  final int SQLCOM_CREATE_EVENT = 124;
  final int SQLCOM_ALTER_EVENT = 125;
  final int SQLCOM_DROP_EVENT = 126;
  final int SQLCOM_SHOW_CREATE_EVENT = 127;
  final int SQLCOM_SHOW_EVENTS = 128;
  final int SQLCOM_SHOW_CREATE_TRIGGER = 129;
  final int SQLCOM_ALTER_DB_UPGRADE = 130;
  final int SQLCOM_SHOW_PROFILE = 131;
  final int SQLCOM_SHOW_PROFILES = 132;
  final int SQLCOM_SIGNAL = 133;
  final int SQLCOM_RESIGNAL = 134;
  final int SQLCOM_SHOW_RELAYLOG_EVENTS = 135;
  final int SQLCOM_GET_DIAGNOSTICS = 136;
  final int SQLCOM_ALTER_USER = 137;
  final int SQLCOM_EXPLAIN_OTHER = 138;
  final int SQLCOM_SHOW_CREATE_USER = 140;
  final int SQLCOM_SHUTDOWN = 141;
  final int SQLCOM_ALTER_INSTANCE = 142;
}
