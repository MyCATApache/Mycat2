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
 * SQL语句分发
 **/
public interface MySQLSQLCommandType {

  int SQLCOM_SELECT = 3;
  int SQLCOM_CREATE_TABLE = 4;
  int SQLCOM_CREATE_INDEX = 5;
  int SQLCOM_ALTER_TABLE = 6;
  int SQLCOM_UPDATE = 7;
  int SQLCOM_INSERT = 8;
  int SQLCOM_INSERT_SELECT = 9;
  int SQLCOM_DELETE = 10;
  int SQLCOM_TRUNCATE = 11;
  int SQLCOM_DROP_TABLE = 12;
  int SQLCOM_DROP_INDEX = 13;
  int SQLCOM_SHOW_DATABASES = 14;
  int SQLCOM_SHOW_TABLES = 15;
  int SQLCOM_SHOW_FIELDS = 16;
  int SQLCOM_SHOW_KEYS = 17;
  int SQLCOM_SHOW_VARIABLES = 18;
  int SQLCOM_SHOW_STATUS = 19;
  int SQLCOM_SHOW_ENGINE_LOGS = 20;
  int SQLCOM_SHOW_ENGINE_STATUS = 21;
  int SQLCOM_SHOW_ENGINE_MUTEX = 22;
  int SQLCOM_SHOW_PROCESSLIST = 23;
  int SQLCOM_SHOW_MASTER_STAT = 24;
  int SQLCOM_SHOW_SLAVE_STAT = 25;
  int SQLCOM_SHOW_GRANTS = 26;
  int SQLCOM_SHOW_CREATE = 27;
  int SQLCOM_SHOW_CHARSETS = 28;
  int SQLCOM_SHOW_COLLATIONS = 29;
  int SQLCOM_SHOW_CREATE_DB = 30;
  int SQLCOM_SHOW_TABLE_STATUS = 31;
  int SQLCOM_SHOW_TRIGGERS = 32;
  int SQLCOM_LOAD = 33;
  int SQLCOM_SET_OPTION = 34;
  int SQLCOM_LOCK_TABLES = 35;
  int SQLCOM_UNLOCK_TABLES = 36;
  int SQLCOM_GRANT = 37;
  int SQLCOM_CHANGE_DB = 38;
  int SQLCOM_CREATE_DB = 39;
  int SQLCOM_DROP_DB = 40;
  int SQLCOM_ALTER_DB = 41;
  int SQLCOM_REPAIR = 42;
  int SQLCOM_REPLACE = 43;
  int SQLCOM_REPLACE_SELECT = 44;
  int SQLCOM_CREATE_FUNCTION = 45;
  int SQLCOM_DROP_FUNCTION = 46;
  int SQLCOM_REVOKE = 47;
  int SQLCOM_OPTIMIZE = 48;
  int SQLCOM_CHECK = 49;
  int SQLCOM_ASSIGN_TO_KEYCACHE = 50;
  int SQLCOM_PRELOAD_KEYS = 51;
  int SQLCOM_FLUSH = 52;
  int SQLCOM_KILL = 53;
  int SQLCOM_ANALYZE = 54;
  int SQLCOM_ROLLBACK = 55;
  int SQLCOM_ROLLBACK_TO_SAVEPOINT = 56;
  int SQLCOM_COMMIT = 57;
  int SQLCOM_SAVEPOINT = 58;
  int SQLCOM_RELEASE_SAVEPOINT = 59;
  int SQLCOM_SLAVE_START = 60;
  int SQLCOM_SLAVE_STOP = 61;
  int SQLCOM_START_GROUP_REPLICATION = 62;
  int SQLCOM_STOP_GROUP_REPLICATION = 63;
  int SQLCOM_BEGIN = 64;
  int SQLCOM_CHANGE_MASTER = 65;
  int SQLCOM_CHANGE_REPLICATION_FILTER = 66;
  int SQLCOM_RENAME_TABLE = 67;
  int SQLCOM_RESET = 68;
  int SQLCOM_PURGE = 69;
  int SQLCOM_PURGE_BEFORE = 70;
  int SQLCOM_SHOW_BINLOGS = 71;
  int SQLCOM_SHOW_OPEN_TABLES = 72;
  int SQLCOM_HA_OPEN = 73;
  int SQLCOM_HA_CLOSE = 74;
  int SQLCOM_HA_READ = 75;
  int SQLCOM_SHOW_SLAVE_HOSTS = 76;
  int SQLCOM_DELETE_MULTI = 77;
  int SQLCOM_UPDATE_MULTI = 78;
  int SQLCOM_SHOW_BINLOG_EVENTS = 79;
  int SQLCOM_DO = 80;
  int SQLCOM_SHOW_WARNS = 81;
  int SQLCOM_EMPTY_QUERY = 82;
  int SQLCOM_SHOW_ERRORS = 83;
  int SQLCOM_SHOW_STORAGE_ENGINES = 84;
  int SQLCOM_SHOW_PRIVILEGES = 85;
  int SQLCOM_HELP = 86;
  int SQLCOM_CREATE_USER = 87;
  int SQLCOM_DROP_USER = 88;
  int SQLCOM_RENAME_USER = 89;
  int SQLCOM_REVOKE_ALL = 90;
  int SQLCOM_CHECKSUM = 91;
  int SQLCOM_CREATE_PROCEDURE = 92;
  int SQLCOM_CREATE_SPFUNCTION = 93;
  int SQLCOM_CALL = 94;
  int SQLCOM_DROP_PROCEDURE = 95;
  int SQLCOM_ALTER_PROCEDURE = 96;
  int SQLCOM_ALTER_FUNCTION = 97;
  int SQLCOM_SHOW_CREATE_PROC = 98;
  int SQLCOM_SHOW_CREATE_FUNC = 99;
  int SQLCOM_SHOW_STATUS_PROC = 100;
  int SQLCOM_SHOW_STATUS_FUNC = 101;
  int SQLCOM_PREPARE = 102;
  int SQLCOM_EXECUTE = 103;
  int SQLCOM_DEALLOCATE_PREPARE = 104;
  int SQLCOM_CREATE_VIEW = 105;
  int SQLCOM_DROP_VIEW = 106;
  int SQLCOM_CREATE_TRIGGER = 107;
  int SQLCOM_DROP_TRIGGER = 108;
  int SQLCOM_XA_START = 109;
  int SQLCOM_XA_END = 110;
  int SQLCOM_XA_COMMIT = 111;
  int SQLCOM_XA_ROLLBACK = 112;
  int SQLCOM_XA_RECOVER = 113;
  int SQLCOM_SHOW_PROC_CODE = 114;
  int SQLCOM_SHOW_FUNC_CODE = 115;
  int SQLCOM_ALTER_TABLESPACE = 116;
  int SQLCOM_INSTALL_PLUGIN = 117;
  int SQLCOM_UNINSTALL_PLUGIN = 118;
  int SQLCOM_BINLOG_BASE64_EVENT = 119;
  int SQLCOM_SHOW_PLUGINS = 120;
  int SQLCOM_CREATE_SERVER = 121;
  int SQLCOM_DROP_SERVER = 122;
  int SQLCOM_ALTER_SERVER = 123;
  int SQLCOM_CREATE_EVENT = 124;
  int SQLCOM_ALTER_EVENT = 125;
  int SQLCOM_DROP_EVENT = 126;
  int SQLCOM_SHOW_CREATE_EVENT = 127;
  int SQLCOM_SHOW_EVENTS = 128;
  int SQLCOM_SHOW_CREATE_TRIGGER = 129;
  int SQLCOM_ALTER_DB_UPGRADE = 130;
  int SQLCOM_SHOW_PROFILE = 131;
  int SQLCOM_SHOW_PROFILES = 132;
  int SQLCOM_SIGNAL = 133;
  int SQLCOM_RESIGNAL = 134;
  int SQLCOM_SHOW_RELAYLOG_EVENTS = 135;
  int SQLCOM_GET_DIAGNOSTICS = 136;
  int SQLCOM_ALTER_USER = 137;
  int SQLCOM_EXPLAIN_OTHER = 138;
  int SQLCOM_SHOW_CREATE_USER = 140;
  int SQLCOM_SHUTDOWN = 141;
  int SQLCOM_ALTER_INSTANCE = 142;

  static void main(String[] args) {
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
}
