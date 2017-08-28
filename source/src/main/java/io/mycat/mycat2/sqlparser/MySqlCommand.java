package io.mycat.mycat2.sqlparser;

/*
@enum  enum_sql_command
@brief SQL Commands

       SQL Command is resolved during SQL parsing and assigned to the Lex
       object= 0; accessible from the THD.

       When a command is added here= 0; be sure it's also added in mysqld.cc
       in "struct show_var_st status_vars[]= {" ...

       If the command returns a result set or is not allowed in stored
       functions or triggers= 0; please also make sure that
       sp_get_flags_for_command (sp_head.cc) returns proper flags for the
       added public static int  SQLCOM_.
*/
public interface MySqlCommand {

	  public static int  SQLCOM_SELECT = 0;
	  public static int  SQLCOM_CREATE_TABLE= 1;
	  public static int  SQLCOM_CREATE_INDEX= 2;
	  public static int  SQLCOM_ALTER_TABLE= 3;
	  public static int  SQLCOM_UPDATE= 4;
	  public static int  SQLCOM_INSERT= 5;
	  public static int  SQLCOM_INSERT_SELECT= 6;
	  public static int  SQLCOM_DELETE= 7;
	  public static int  SQLCOM_TRUNCATE= 8;
	  public static int  SQLCOM_DROP_TABLE= 9;
	  public static int  SQLCOM_DROP_INDEX= 10;
	  public static int  SQLCOM_SHOW_DATABASES= 11;
	  public static int  SQLCOM_SHOW_TABLES= 12;
	  public static int  SQLCOM_SHOW_FIELDS= 13;
	  public static int  SQLCOM_SHOW_KEYS= 14;
	  public static int  SQLCOM_SHOW_VARIABLES= 15;
	  public static int  SQLCOM_SHOW_STATUS= 16;
	  public static int  SQLCOM_SHOW_ENGINE_LOGS= 17;
	  public static int  SQLCOM_SHOW_ENGINE_STATUS= 18;
	  public static int  SQLCOM_SHOW_ENGINE_MUTEX= 19;
	  public static int  SQLCOM_SHOW_PROCESSLIST= 20;
	  public static int  SQLCOM_SHOW_MASTER_STAT= 21;
	  public static int  SQLCOM_SHOW_SLAVE_STAT= 22;
	  public static int  SQLCOM_SHOW_GRANTS= 23;
	  public static int  SQLCOM_SHOW_CREATE= 24;
	  public static int  SQLCOM_SHOW_CHARSETS= 25;
	  public static int  SQLCOM_SHOW_COLLATIONS= 26;
	  public static int  SQLCOM_SHOW_CREATE_DB= 27;
	  public static int  SQLCOM_SHOW_TABLE_STATUS= 28;
	  public static int  SQLCOM_SHOW_TRIGGERS= 29;
	  public static int  SQLCOM_LOAD= 30;
	  public static int  SQLCOM_SET_OPTION= 31;
	  public static int  SQLCOM_LOCK_TABLES= 32;
	  public static int  SQLCOM_UNLOCK_TABLES= 33;
	  public static int  SQLCOM_GRANT= 34;
	  public static int  SQLCOM_CHANGE_DB= 35;
	  public static int  SQLCOM_CREATE_DB= 36;
	  public static int  SQLCOM_DROP_DB= 37;
	  public static int  SQLCOM_ALTER_DB= 38;
	  public static int  SQLCOM_REPAIR= 39;
	  public static int  SQLCOM_REPLACE= 40;
	  public static int  SQLCOM_REPLACE_SELECT= 41;
	  public static int  SQLCOM_CREATE_FUNCTION= 42;
	  public static int  SQLCOM_DROP_FUNCTION= 43;
	  public static int  SQLCOM_REVOKE= 44;
	  public static int  SQLCOM_OPTIMIZE= 45;
	  public static int  SQLCOM_CHECK= 46;
	  public static int  SQLCOM_ASSIGN_TO_KEYCACHE= 47;
	  public static int  SQLCOM_PRELOAD_KEYS= 48;
	  public static int  SQLCOM_FLUSH= 49;
	  public static int  SQLCOM_KILL= 50;
	  public static int  SQLCOM_ANALYZE= 51;
	  public static int  SQLCOM_ROLLBACK= 52;
	  public static int  SQLCOM_ROLLBACK_TO_SAVEPOINT= 53;
	  public static int  SQLCOM_COMMIT= 54;
	  public static int  SQLCOM_SAVEPOINT= 55;
	  public static int  SQLCOM_RELEASE_SAVEPOINT= 56;
	  public static int  SQLCOM_SLAVE_START= 57;
	  public static int  SQLCOM_SLAVE_STOP= 58;
	  public static int  SQLCOM_START_GROUP_REPLICATION= 59;
	  public static int  SQLCOM_STOP_GROUP_REPLICATION= 60;
	  public static int  SQLCOM_BEGIN= 61;
	  public static int  SQLCOM_CHANGE_MASTER= 62;
	  public static int  SQLCOM_CHANGE_REPLICATION_FILTER= 63;
	  public static int  SQLCOM_RENAME_TABLE= 64;
	  public static int  SQLCOM_RESET= 65;
	  public static int  SQLCOM_PURGE= 66;
	  public static int  SQLCOM_PURGE_BEFORE= 67;
	  public static int  SQLCOM_SHOW_BINLOGS= 68;
	  public static int  SQLCOM_SHOW_OPEN_TABLES= 69;
	  public static int  SQLCOM_HA_OPEN= 70;
	  public static int  SQLCOM_HA_CLOSE= 71;
	  public static int  SQLCOM_HA_READ= 72;
	  public static int  SQLCOM_SHOW_SLAVE_HOSTS= 73;
	  public static int  SQLCOM_DELETE_MULTI= 74;
	  public static int  SQLCOM_UPDATE_MULTI= 75;
	  public static int  SQLCOM_SHOW_BINLOG_EVENTS= 76;
	  public static int  SQLCOM_DO= 77;
	  public static int  SQLCOM_SHOW_WARNS= 78;
	  public static int  SQLCOM_EMPTY_QUERY= 79;
	  public static int  SQLCOM_SHOW_ERRORS= 80;
	  public static int  SQLCOM_SHOW_STORAGE_ENGINES= 81;
	  public static int  SQLCOM_SHOW_PRIVILEGES= 82;
	  public static int  SQLCOM_HELP= 83;
	  public static int  SQLCOM_CREATE_USER= 84;
	  public static int  SQLCOM_DROP_USER= 85;
	  public static int  SQLCOM_RENAME_USER= 86;
	  public static int  SQLCOM_REVOKE_ALL= 87;
	  public static int  SQLCOM_CHECKSUM= 88;
	  public static int  SQLCOM_CREATE_PROCEDURE= 89;
	  public static int  SQLCOM_CREATE_SPFUNCTION= 90;
	  public static int  SQLCOM_CALL= 91;
	  public static int  SQLCOM_DROP_PROCEDURE= 92;
	  public static int  SQLCOM_ALTER_PROCEDURE= 93;
	  public static int  SQLCOM_ALTER_FUNCTION= 94;
	  public static int  SQLCOM_SHOW_CREATE_PROC= 95;
	  public static int  SQLCOM_SHOW_CREATE_FUNC= 96;
	  public static int  SQLCOM_SHOW_STATUS_PROC= 97;
	  public static int  SQLCOM_SHOW_STATUS_FUNC= 98;
	  public static int  SQLCOM_PREPARE= 99;
	  public static int  SQLCOM_EXECUTE= 100;
	  public static int  SQLCOM_DEALLOCATE_PREPARE= 101;
	  public static int  SQLCOM_CREATE_VIEW= 102;
	  public static int  SQLCOM_DROP_VIEW= 103;
	  public static int  SQLCOM_CREATE_TRIGGER= 104;
	  public static int  SQLCOM_DROP_TRIGGER= 105;
	  public static int  SQLCOM_XA_START= 106;
	  public static int  SQLCOM_XA_END= 107;
	  public static int  SQLCOM_XA_PREPARE= 108;
	  public static int  SQLCOM_XA_COMMIT= 109;
	  public static int  SQLCOM_XA_ROLLBACK= 110;
	  public static int  SQLCOM_XA_RECOVER= 111;
	  public static int  SQLCOM_SHOW_PROC_CODE= 112;
	  public static int  SQLCOM_SHOW_FUNC_CODE= 113;
	  public static int  SQLCOM_ALTER_TABLESPACE= 114;
	  public static int  SQLCOM_INSTALL_PLUGIN= 115;
	  public static int  SQLCOM_UNINSTALL_PLUGIN= 116;
	  public static int  SQLCOM_BINLOG_BASE64_EVENT= 117;
	  public static int  SQLCOM_SHOW_PLUGINS= 118;
	  public static int  SQLCOM_CREATE_SERVER= 119;
	  public static int  SQLCOM_DROP_SERVER= 120;
	  public static int  SQLCOM_ALTER_SERVER= 121;
	  public static int  SQLCOM_CREATE_EVENT= 122;
	  public static int  SQLCOM_ALTER_EVENT= 123;
	  public static int  SQLCOM_DROP_EVENT= 124;
	  public static int  SQLCOM_SHOW_CREATE_EVENT= 125;
	  public static int  SQLCOM_SHOW_EVENTS= 126;
	  public static int  SQLCOM_SHOW_CREATE_TRIGGER= 127;
	  public static int  SQLCOM_ALTER_DB_UPGRADE= 128;
	  public static int  SQLCOM_SHOW_PROFILE= 129;
	  public static int  SQLCOM_SHOW_PROFILES= 130;
	  public static int  SQLCOM_SIGNAL= 131;
	  public static int  SQLCOM_RESIGNAL= 132;
	  public static int  SQLCOM_SHOW_RELAYLOG_EVENTS= 133;
	  public static int  SQLCOM_GET_DIAGNOSTICS= 134;
	  public static int  SQLCOM_ALTER_USER= 135;
	  public static int  SQLCOM_EXPLAIN_OTHER= 136;
	  public static int  SQLCOM_SHOW_CREATE_USER= 137;
	  public static int  SQLCOM_SHUTDOWN= 138;
	  public static int  SQLCOM_ALTER_INSTANCE= 139;
	  /* This should be the last !!! */
	  public static int  SQLCOM_END = 140;
}
