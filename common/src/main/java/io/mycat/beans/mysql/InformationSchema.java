package io.mycat.beans.mysql;


import java.math.BigDecimal;
import java.util.List;

/**
 * @author Junwen Chen
 **/
public class InformationSchema {

    public List TABLES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_COLUMNS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_TABLESPACES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_FT_INDEX_TABLE = new java.util.concurrent.CopyOnWriteArrayList();

    public List CHECK_CONSTRAINTS = new java.util.concurrent.CopyOnWriteArrayList();

    public List PARTITIONS = new java.util.concurrent.CopyOnWriteArrayList();

    public List TABLE_PRIVILEGES = new java.util.concurrent.CopyOnWriteArrayList();

    public List TRIGGERS = new java.util.concurrent.CopyOnWriteArrayList();

    public List GEOMETRY_COLUMNS = new java.util.concurrent.CopyOnWriteArrayList();

    public List EVENTS = new java.util.concurrent.CopyOnWriteArrayList();

    public List PARAMETERS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_FT_DEFAULT_STOPWORD = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_TABLESPACES_SCRUBBING = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_LOCK_WAITS = new java.util.concurrent.CopyOnWriteArrayList();

    public List FILES = new java.util.concurrent.CopyOnWriteArrayList();

    public List PLUGINS = new java.util.concurrent.CopyOnWriteArrayList();

    public List GLOBAL_STATUS = new java.util.concurrent.CopyOnWriteArrayList();

    public List ALL_PLUGINS = new java.util.concurrent.CopyOnWriteArrayList();

    public List USER_STATISTICS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_TABLESTATS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_SEMAPHORE_WAITS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_CMP_PER_INDEX_RESET = new java.util.concurrent.CopyOnWriteArrayList();

    public List TABLE_STATISTICS = new java.util.concurrent.CopyOnWriteArrayList();

    public List COLLATION_CHARACTER_SET_APPLICABILITY = new java.util.concurrent.CopyOnWriteArrayList();

    public List ENGINES = new java.util.concurrent.CopyOnWriteArrayList();

    public List KEY_COLUMN_USAGE = new java.util.concurrent.CopyOnWriteArrayList();

    public List GLOBAL_VARIABLES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_LOCKS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_FOREIGN_COLS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_BUFFER_PAGE = new java.util.concurrent.CopyOnWriteArrayList();

    public List SESSION_STATUS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_DATAFILES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_CMP = new java.util.concurrent.CopyOnWriteArrayList();

    public List KEY_CACHES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_CMPMEM_RESET = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_VIRTUAL = new java.util.concurrent.CopyOnWriteArrayList();

    public List APPLICABLE_ROLES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_FIELDS = new java.util.concurrent.CopyOnWriteArrayList();

    public List TABLESPACES = new java.util.concurrent.CopyOnWriteArrayList();

    public List REFERENTIAL_CONSTRAINTS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_TABLES = new java.util.concurrent.CopyOnWriteArrayList();

    public List SCHEMATA = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_FT_BEING_DELETED = new java.util.concurrent.CopyOnWriteArrayList();

    public List ENABLED_ROLES = new java.util.concurrent.CopyOnWriteArrayList();

    public List COLUMNS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_BUFFER_POOL_STATS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_CMP_PER_INDEX = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_MUTEXES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_BUFFER_PAGE_LRU = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_FT_CONFIG = new java.util.concurrent.CopyOnWriteArrayList();

    public List SYSTEM_VARIABLES = new java.util.concurrent.CopyOnWriteArrayList();

    public List TABLE_CONSTRAINTS = new java.util.concurrent.CopyOnWriteArrayList();

    public List CLIENT_STATISTICS = new java.util.concurrent.CopyOnWriteArrayList();

    public List PROFILING = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_TABLESPACES_ENCRYPTION = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_FOREIGN = new java.util.concurrent.CopyOnWriteArrayList();

    public List COLLATIONS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_CMPMEM = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_TRX = new java.util.concurrent.CopyOnWriteArrayList();

    public List CHARACTER_SETS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INDEX_STATISTICS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_FT_DELETED = new java.util.concurrent.CopyOnWriteArrayList();

    public List STATISTICS = new java.util.concurrent.CopyOnWriteArrayList();

    public List VIEWS = new java.util.concurrent.CopyOnWriteArrayList();

    public List COLUMN_PRIVILEGES = new java.util.concurrent.CopyOnWriteArrayList();

    public List user_variables = new java.util.concurrent.CopyOnWriteArrayList();

    public List SESSION_VARIABLES = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_METRICS = new java.util.concurrent.CopyOnWriteArrayList();

    public List SPATIAL_REF_SYS = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_CMP_RESET = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_FT_INDEX_CACHE = new java.util.concurrent.CopyOnWriteArrayList();

    public List INNODB_SYS_INDEXES = new java.util.concurrent.CopyOnWriteArrayList();

    public List USER_PRIVILEGES = new java.util.concurrent.CopyOnWriteArrayList();

    public List PROCESSLIST = new java.util.concurrent.CopyOnWriteArrayList();

    public List SCHEMA_PRIVILEGES = new java.util.concurrent.CopyOnWriteArrayList();

    public List ROUTINES = new java.util.concurrent.CopyOnWriteArrayList();

    public static class TABLES {
        public static final String createTableSQL = "CREATE TABLE `TABLES` (\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tENGINE varchar(64) DEFAULT NULL,\n"
                + "\tVERSION bigint(21) DEFAULT NULL,\n"
                + "\tROW_FORMAT varchar(10) DEFAULT NULL,\n"
                + "\tTABLE_ROWS bigint(21) DEFAULT NULL,\n"
                + "\tAVG_ROW_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tDATA_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tMAX_DATA_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tINDEX_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tDATA_FREE bigint(21) DEFAULT NULL,\n"
                + "\tAUTO_INCREMENT bigint(21) DEFAULT NULL,\n"
                + "\tCREATE_TIME datetime DEFAULT NULL,\n"
                + "\tUPDATE_TIME datetime DEFAULT NULL,\n"
                + "\tCHECK_TIME datetime DEFAULT NULL,\n"
                + "\tTABLE_COLLATION varchar(32) DEFAULT NULL,\n"
                + "\tCHECKSUM bigint(21) DEFAULT NULL,\n"
                + "\tCREATE_OPTIONS varchar(2048) DEFAULT NULL,\n"
                + "\tTABLE_COMMENT varchar(2048) DEFAULT '' NOT NULL,\n"
                + "\tMAX_INDEX_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tTEMPORARY varchar(1) DEFAULT NULL\n"
                + ")";

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String TABLE_TYPE;

        public String ENGINE;

        public Long VERSION;

        public String ROW_FORMAT;

        public Long TABLE_ROWS;

        public Long AVG_ROW_LENGTH;

        public Long DATA_LENGTH;

        public Long MAX_DATA_LENGTH;

        public Long INDEX_LENGTH;

        public Long DATA_FREE;

        public Long AUTO_INCREMENT;

        public Long CREATE_TIME;

        public Long UPDATE_TIME;

        public Long CHECK_TIME;

        public String TABLE_COLLATION;

        public Long CHECKSUM;

        public String CREATE_OPTIONS;

        public String TABLE_COMMENT;

        public Long MAX_INDEX_LENGTH;

        public String TEMPORARY;
    }

    public static class INNODB_SYS_COLUMNS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_COLUMNS` (\n"
                + "\tTABLE_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tPOS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMTYPE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tPRTYPE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tLEN int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long TABLE_ID;

        public String NAME;

        public Long POS;

        public Integer MTYPE;

        public Integer PRTYPE;

        public Integer LEN;
    }

    public static class INNODB_SYS_TABLESPACES {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_TABLESPACES` (\n"
                + "\tSPACE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(655) DEFAULT '' NOT NULL,\n"
                + "\tFLAG int(11) DEFAULT 0 NOT NULL,\n"
                + "\tROW_FORMAT varchar(22) DEFAULT NULL,\n"
                + "\tPAGE_SIZE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tZIP_PAGE_SIZE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tSPACE_TYPE varchar(10) DEFAULT NULL,\n"
                + "\tFS_BLOCK_SIZE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tFILE_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tALLOCATED_SIZE bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public Integer SPACE;

        public String NAME;

        public Integer FLAG;

        public String ROW_FORMAT;

        public Integer PAGE_SIZE;

        public Integer ZIP_PAGE_SIZE;

        public String SPACE_TYPE;

        public Integer FS_BLOCK_SIZE;

        public Long FILE_SIZE;

        public Long ALLOCATED_SIZE;
    }

    public static class INNODB_FT_INDEX_TABLE {
        public static final String createTableSQL = "CREATE TABLE `INNODB_FT_INDEX_TABLE` (\n"
                + "\tWORD varchar(337) DEFAULT '' NOT NULL,\n"
                + "\tFIRST_DOC_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLAST_DOC_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDOC_COUNT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDOC_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPOSITION bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String WORD;

        public Long FIRST_DOC_ID;

        public Long LAST_DOC_ID;

        public Long DOC_COUNT;

        public Long DOC_ID;

        public Long POSITION;
    }

    public static class CHECK_CONSTRAINTS {
        public static final String createTableSQL = "CREATE TABLE `CHECK_CONSTRAINTS` (\n"
                + "\tCONSTRAINT_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCHECK_CLAUSE varchar(64) DEFAULT '' NOT NULL\n"
                + ")";

        public String CONSTRAINT_CATALOG;

        public String CONSTRAINT_SCHEMA;

        public String CONSTRAINT_NAME;

        public String TABLE_NAME;

        public String CHECK_CLAUSE;
    }

    public static class PARTITIONS {
        public static final String createTableSQL = "CREATE TABLE `PARTITIONS` (\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPARTITION_NAME varchar(64) DEFAULT NULL,\n"
                + "\tSUBPARTITION_NAME varchar(64) DEFAULT NULL,\n"
                + "\tPARTITION_ORDINAL_POSITION bigint(21) DEFAULT NULL,\n"
                + "\tSUBPARTITION_ORDINAL_POSITION bigint(21) DEFAULT NULL,\n"
                + "\tPARTITION_METHOD varchar(18) DEFAULT NULL,\n"
                + "\tSUBPARTITION_METHOD varchar(12) DEFAULT NULL,\n"
                + "\tPARTITION_EXPRESSION longtext DEFAULT NULL,\n"
                + "\tSUBPARTITION_EXPRESSION longtext DEFAULT NULL,\n"
                + "\tPARTITION_DESCRIPTION longtext DEFAULT NULL,\n"
                + "\tTABLE_ROWS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tAVG_ROW_LENGTH bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDATA_LENGTH bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_DATA_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tINDEX_LENGTH bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDATA_FREE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCREATE_TIME datetime DEFAULT NULL,\n"
                + "\tUPDATE_TIME datetime DEFAULT NULL,\n"
                + "\tCHECK_TIME datetime DEFAULT NULL,\n"
                + "\tCHECKSUM bigint(21) DEFAULT NULL,\n"
                + "\tPARTITION_COMMENT varchar(80) DEFAULT '' NOT NULL,\n"
                + "\tNODEGROUP varchar(12) DEFAULT '' NOT NULL,\n"
                + "\tTABLESPACE_NAME varchar(64) DEFAULT NULL\n"
                + ")";

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String PARTITION_NAME;

        public String SUBPARTITION_NAME;

        public Long PARTITION_ORDINAL_POSITION;

        public Long SUBPARTITION_ORDINAL_POSITION;

        public String PARTITION_METHOD;

        public String SUBPARTITION_METHOD;

        public String PARTITION_EXPRESSION;

        public String SUBPARTITION_EXPRESSION;

        public String PARTITION_DESCRIPTION;

        public Long TABLE_ROWS;

        public Long AVG_ROW_LENGTH;

        public Long DATA_LENGTH;

        public Long MAX_DATA_LENGTH;

        public Long INDEX_LENGTH;

        public Long DATA_FREE;

        public Long CREATE_TIME;

        public Long UPDATE_TIME;

        public Long CHECK_TIME;

        public Long CHECKSUM;

        public String PARTITION_COMMENT;

        public String NODEGROUP;

        public String TABLESPACE_NAME;
    }

    public static class TABLE_PRIVILEGES {
        public static final String createTableSQL = "CREATE TABLE `TABLE_PRIVILEGES` (\n"
                + "\tGRANTEE varchar(190) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPRIVILEGE_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tIS_GRANTABLE varchar(3) DEFAULT '' NOT NULL\n"
                + ")";

        public String GRANTEE;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    public static class TRIGGERS {
        public static final String createTableSQL = "CREATE TABLE `TRIGGERS` (\n"
                + "\tTRIGGER_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTRIGGER_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTRIGGER_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_MANIPULATION varchar(6) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_OBJECT_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_OBJECT_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_OBJECT_TABLE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tACTION_ORDER bigint(4) DEFAULT 0 NOT NULL,\n"
                + "\tACTION_CONDITION longtext DEFAULT NULL,\n"
                + "\tACTION_STATEMENT longtext DEFAULT '' NOT NULL,\n"
                + "\tACTION_ORIENTATION varchar(9) DEFAULT '' NOT NULL,\n"
                + "\tACTION_TIMING varchar(6) DEFAULT '' NOT NULL,\n"
                + "\tACTION_REFERENCE_OLD_TABLE varchar(64) DEFAULT NULL,\n"
                + "\tACTION_REFERENCE_NEW_TABLE varchar(64) DEFAULT NULL,\n"
                + "\tACTION_REFERENCE_OLD_ROW varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tACTION_REFERENCE_NEW_ROW varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tCREATED datetime(2) DEFAULT NULL,\n"
                + "\tSQL_MODE varchar(8192) DEFAULT '' NOT NULL,\n"
                + "\tDEFINER varchar(189) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_SET_CLIENT varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tCOLLATION_CONNECTION varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tDATABASE_COLLATION varchar(32) DEFAULT '' NOT NULL\n"
                + ")";

        public String TRIGGER_CATALOG;

        public String TRIGGER_SCHEMA;

        public String TRIGGER_NAME;

        public String EVENT_MANIPULATION;

        public String EVENT_OBJECT_CATALOG;

        public String EVENT_OBJECT_SCHEMA;

        public String EVENT_OBJECT_TABLE;

        public Long ACTION_ORDER;

        public String ACTION_CONDITION;

        public String ACTION_STATEMENT;

        public String ACTION_ORIENTATION;

        public String ACTION_TIMING;

        public String ACTION_REFERENCE_OLD_TABLE;

        public String ACTION_REFERENCE_NEW_TABLE;

        public String ACTION_REFERENCE_OLD_ROW;

        public String ACTION_REFERENCE_NEW_ROW;

        public Long CREATED;

        public String SQL_MODE;

        public String DEFINER;

        public String CHARACTER_SET_CLIENT;

        public String COLLATION_CONNECTION;

        public String DATABASE_COLLATION;
    }

    public static class GEOMETRY_COLUMNS {
        public static final String createTableSQL = "CREATE TABLE `GEOMETRY_COLUMNS` (\n"
                + "\tF_TABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tF_TABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tF_TABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tF_GEOMETRY_COLUMN varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tG_TABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tG_TABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tG_TABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tG_GEOMETRY_COLUMN varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tSTORAGE_TYPE tinyint(2) DEFAULT 0 NOT NULL,\n"
                + "\tGEOMETRY_TYPE int(7) DEFAULT 0 NOT NULL,\n"
                + "\tCOORD_DIMENSION tinyint(2) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_PPR tinyint(2) DEFAULT 0 NOT NULL,\n"
                + "\tSRID smallint(5) DEFAULT 0 NOT NULL\n"
                + ")";

        public String F_TABLE_CATALOG;

        public String F_TABLE_SCHEMA;

        public String F_TABLE_NAME;

        public String F_GEOMETRY_COLUMN;

        public String G_TABLE_CATALOG;

        public String G_TABLE_SCHEMA;

        public String G_TABLE_NAME;

        public String G_GEOMETRY_COLUMN;

        public Short STORAGE_TYPE;

        public Integer GEOMETRY_TYPE;

        public Short COORD_DIMENSION;

        public Short MAX_PPR;

        public Short SRID;
    }

    public static class EVENTS {
        public static final String createTableSQL = "CREATE TABLE `EVENTS` (\n"
                + "\tEVENT_CATALOG varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tDEFINER varchar(189) DEFAULT '' NOT NULL,\n"
                + "\tTIME_ZONE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_BODY varchar(8) DEFAULT '' NOT NULL,\n"
                + "\tEVENT_DEFINITION longtext DEFAULT '' NOT NULL,\n"
                + "\tEVENT_TYPE varchar(9) DEFAULT '' NOT NULL,\n"
                + "\tEXECUTE_AT datetime DEFAULT NULL,\n"
                + "\tINTERVAL_VALUE varchar(256) DEFAULT NULL,\n"
                + "\tINTERVAL_FIELD varchar(18) DEFAULT NULL,\n"
                + "\tSQL_MODE varchar(8192) DEFAULT '' NOT NULL,\n"
                + "\tSTARTS datetime DEFAULT NULL,\n"
                + "\tENDS datetime DEFAULT NULL,\n"
                + "\tSTATUS varchar(18) DEFAULT '' NOT NULL,\n"
                + "\tON_COMPLETION varchar(12) DEFAULT '' NOT NULL,\n"
                + "\tCREATED datetime DEFAULT '0000-00-00 00:00:00' NOT NULL,\n"
                + "\tLAST_ALTERED datetime DEFAULT '0000-00-00 00:00:00' NOT NULL,\n"
                + "\tLAST_EXECUTED datetime DEFAULT NULL,\n"
                + "\tEVENT_COMMENT varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tORIGINATOR bigint(10) DEFAULT 0 NOT NULL,\n"
                + "\tCHARACTER_SET_CLIENT varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tCOLLATION_CONNECTION varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tDATABASE_COLLATION varchar(32) DEFAULT '' NOT NULL\n"
                + ")";

        public String EVENT_CATALOG;

        public String EVENT_SCHEMA;

        public String EVENT_NAME;

        public String DEFINER;

        public String TIME_ZONE;

        public String EVENT_BODY;

        public String EVENT_DEFINITION;

        public String EVENT_TYPE;

        public Long EXECUTE_AT;

        public String INTERVAL_VALUE;

        public String INTERVAL_FIELD;

        public String SQL_MODE;

        public Long STARTS;

        public Long ENDS;

        public String STATUS;

        public String ON_COMPLETION;

        public Long CREATED;

        public Long LAST_ALTERED;

        public Long LAST_EXECUTED;

        public String EVENT_COMMENT;

        public Long ORIGINATOR;

        public String CHARACTER_SET_CLIENT;

        public String COLLATION_CONNECTION;

        public String DATABASE_COLLATION;
    }

    public static class PARAMETERS {
        public static final String createTableSQL = "CREATE TABLE `PARAMETERS` (\n"
                + "\tSPECIFIC_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tSPECIFIC_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tSPECIFIC_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tORDINAL_POSITION int(21) DEFAULT 0 NOT NULL,\n"
                + "\tPARAMETER_MODE varchar(5) DEFAULT NULL,\n"
                + "\tPARAMETER_NAME varchar(64) DEFAULT NULL,\n"
                + "\tDATA_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_MAXIMUM_LENGTH int(21) DEFAULT NULL,\n"
                + "\tCHARACTER_OCTET_LENGTH int(21) DEFAULT NULL,\n"
                + "\tNUMERIC_PRECISION int(21) DEFAULT NULL,\n"
                + "\tNUMERIC_SCALE int(21) DEFAULT NULL,\n"
                + "\tDATETIME_PRECISION bigint(21) DEFAULT NULL,\n"
                + "\tCHARACTER_SET_NAME varchar(64) DEFAULT NULL,\n"
                + "\tCOLLATION_NAME varchar(64) DEFAULT NULL,\n"
                + "\tDTD_IDENTIFIER longtext DEFAULT '' NOT NULL,\n"
                + "\tROUTINE_TYPE varchar(9) DEFAULT '' NOT NULL\n"
                + ")";

        public String SPECIFIC_CATALOG;

        public String SPECIFIC_SCHEMA;

        public String SPECIFIC_NAME;

        public Integer ORDINAL_POSITION;

        public String PARAMETER_MODE;

        public String PARAMETER_NAME;

        public String DATA_TYPE;

        public Integer CHARACTER_MAXIMUM_LENGTH;

        public Integer CHARACTER_OCTET_LENGTH;

        public Integer NUMERIC_PRECISION;

        public Integer NUMERIC_SCALE;

        public Long DATETIME_PRECISION;

        public String CHARACTER_SET_NAME;

        public String COLLATION_NAME;

        public String DTD_IDENTIFIER;

        public String ROUTINE_TYPE;
    }

    public static class INNODB_FT_DEFAULT_STOPWORD {
        public static final String createTableSQL = "CREATE TABLE `INNODB_FT_DEFAULT_STOPWORD` (\n"
                + "\tvalue varchar(18) DEFAULT '' NOT NULL\n"
                + ")";

        public String value;
    }

    public static class INNODB_TABLESPACES_SCRUBBING {
        public static final String createTableSQL = "CREATE TABLE `INNODB_TABLESPACES_SCRUBBING` (\n"
                + "\tSPACE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(655) DEFAULT NULL,\n"
                + "\tCOMPRESSED int(11) DEFAULT 0 NOT NULL,\n"
                + "\tLAST_SCRUB_COMPLETED datetime DEFAULT NULL,\n"
                + "\tCURRENT_SCRUB_STARTED datetime DEFAULT NULL,\n"
                + "\tCURRENT_SCRUB_ACTIVE_THREADS int(11) DEFAULT NULL,\n"
                + "\tCURRENT_SCRUB_PAGE_NUMBER bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCURRENT_SCRUB_MAX_PAGE_NUMBER bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long SPACE;

        public String NAME;

        public Integer COMPRESSED;

        public Long LAST_SCRUB_COMPLETED;

        public Long CURRENT_SCRUB_STARTED;

        public Integer CURRENT_SCRUB_ACTIVE_THREADS;

        public Long CURRENT_SCRUB_PAGE_NUMBER;

        public Long CURRENT_SCRUB_MAX_PAGE_NUMBER;
    }

    public static class INNODB_LOCK_WAITS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_LOCK_WAITS` (\n"
                + "\trequesting_trx_id varchar(18) DEFAULT '' NOT NULL,\n"
                + "\trequested_lock_id varchar(81) DEFAULT '' NOT NULL,\n"
                + "\tblocking_trx_id varchar(18) DEFAULT '' NOT NULL,\n"
                + "\tblocking_lock_id varchar(81) DEFAULT '' NOT NULL\n"
                + ")";

        public String requesting_trx_id;

        public String requested_lock_id;

        public String blocking_trx_id;

        public String blocking_lock_id;
    }

    public static class FILES {
        public static final String createTableSQL = "CREATE TABLE `FILES` (\n"
                + "\tFILE_ID bigint(4) DEFAULT 0 NOT NULL,\n"
                + "\tFILE_NAME varchar(512) DEFAULT NULL,\n"
                + "\tFILE_TYPE varchar(20) DEFAULT '' NOT NULL,\n"
                + "\tTABLESPACE_NAME varchar(64) DEFAULT NULL,\n"
                + "\tTABLE_CATALOG varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT NULL,\n"
                + "\tLOGFILE_GROUP_NAME varchar(64) DEFAULT NULL,\n"
                + "\tLOGFILE_GROUP_NUMBER bigint(4) DEFAULT NULL,\n"
                + "\tENGINE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tFULLTEXT_KEYS varchar(64) DEFAULT NULL,\n"
                + "\tDELETED_ROWS bigint(4) DEFAULT NULL,\n"
                + "\tUPDATE_COUNT bigint(4) DEFAULT NULL,\n"
                + "\tFREE_EXTENTS bigint(4) DEFAULT NULL,\n"
                + "\tTOTAL_EXTENTS bigint(4) DEFAULT NULL,\n"
                + "\tEXTENT_SIZE bigint(4) DEFAULT 0 NOT NULL,\n"
                + "\tINITIAL_SIZE bigint(21) DEFAULT NULL,\n"
                + "\tMAXIMUM_SIZE bigint(21) DEFAULT NULL,\n"
                + "\tAUTOEXTEND_SIZE bigint(21) DEFAULT NULL,\n"
                + "\tCREATION_TIME datetime DEFAULT NULL,\n"
                + "\tLAST_UPDATE_TIME datetime DEFAULT NULL,\n"
                + "\tLAST_ACCESS_TIME datetime DEFAULT NULL,\n"
                + "\tRECOVER_TIME bigint(4) DEFAULT NULL,\n"
                + "\tTRANSACTION_COUNTER bigint(4) DEFAULT NULL,\n"
                + "\tVERSION bigint(21) DEFAULT NULL,\n"
                + "\tROW_FORMAT varchar(10) DEFAULT NULL,\n"
                + "\tTABLE_ROWS bigint(21) DEFAULT NULL,\n"
                + "\tAVG_ROW_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tDATA_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tMAX_DATA_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tINDEX_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tDATA_FREE bigint(21) DEFAULT NULL,\n"
                + "\tCREATE_TIME datetime DEFAULT NULL,\n"
                + "\tUPDATE_TIME datetime DEFAULT NULL,\n"
                + "\tCHECK_TIME datetime DEFAULT NULL,\n"
                + "\tCHECKSUM bigint(21) DEFAULT NULL,\n"
                + "\tSTATUS varchar(20) DEFAULT '' NOT NULL,\n"
                + "\tEXTRA varchar(255) DEFAULT NULL\n"
                + ")";

        public Long FILE_ID;

        public String FILE_NAME;

        public String FILE_TYPE;

        public String TABLESPACE_NAME;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String LOGFILE_GROUP_NAME;

        public Long LOGFILE_GROUP_NUMBER;

        public String ENGINE;

        public String FULLTEXT_KEYS;

        public Long DELETED_ROWS;

        public Long UPDATE_COUNT;

        public Long FREE_EXTENTS;

        public Long TOTAL_EXTENTS;

        public Long EXTENT_SIZE;

        public Long INITIAL_SIZE;

        public Long MAXIMUM_SIZE;

        public Long AUTOEXTEND_SIZE;

        public Long CREATION_TIME;

        public Long LAST_UPDATE_TIME;

        public Long LAST_ACCESS_TIME;

        public Long RECOVER_TIME;

        public Long TRANSACTION_COUNTER;

        public Long VERSION;

        public String ROW_FORMAT;

        public Long TABLE_ROWS;

        public Long AVG_ROW_LENGTH;

        public Long DATA_LENGTH;

        public Long MAX_DATA_LENGTH;

        public Long INDEX_LENGTH;

        public Long DATA_FREE;

        public Long CREATE_TIME;

        public Long UPDATE_TIME;

        public Long CHECK_TIME;

        public Long CHECKSUM;

        public String STATUS;

        public String EXTRA;
    }

    public static class PLUGINS {
        public static final String createTableSQL = "CREATE TABLE `PLUGINS` (\n"
                + "\tPLUGIN_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_VERSION varchar(20) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_STATUS varchar(16) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_TYPE varchar(80) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_TYPE_VERSION varchar(20) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_LIBRARY varchar(64) DEFAULT NULL,\n"
                + "\tPLUGIN_LIBRARY_VERSION varchar(20) DEFAULT NULL,\n"
                + "\tPLUGIN_AUTHOR varchar(64) DEFAULT NULL,\n"
                + "\tPLUGIN_DESCRIPTION longtext DEFAULT NULL,\n"
                + "\tPLUGIN_LICENSE varchar(80) DEFAULT '' NOT NULL,\n"
                + "\tLOAD_OPTION varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_MATURITY varchar(12) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_AUTH_VERSION varchar(80) DEFAULT NULL\n"
                + ")";

        public String PLUGIN_NAME;

        public String PLUGIN_VERSION;

        public String PLUGIN_STATUS;

        public String PLUGIN_TYPE;

        public String PLUGIN_TYPE_VERSION;

        public String PLUGIN_LIBRARY;

        public String PLUGIN_LIBRARY_VERSION;

        public String PLUGIN_AUTHOR;

        public String PLUGIN_DESCRIPTION;

        public String PLUGIN_LICENSE;

        public String LOAD_OPTION;

        public String PLUGIN_MATURITY;

        public String PLUGIN_AUTH_VERSION;
    }

    public static class GLOBAL_STATUS {
        public static final String createTableSQL = "CREATE TABLE `GLOBAL_STATUS` (\n"
                + "\tVARIABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVARIABLE_VALUE varchar(2048) DEFAULT '' NOT NULL\n"
                + ")";

        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    public static class ALL_PLUGINS {
        public static final String createTableSQL = "CREATE TABLE `ALL_PLUGINS` (\n"
                + "\tPLUGIN_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_VERSION varchar(20) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_STATUS varchar(16) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_TYPE varchar(80) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_TYPE_VERSION varchar(20) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_LIBRARY varchar(64) DEFAULT NULL,\n"
                + "\tPLUGIN_LIBRARY_VERSION varchar(20) DEFAULT NULL,\n"
                + "\tPLUGIN_AUTHOR varchar(64) DEFAULT NULL,\n"
                + "\tPLUGIN_DESCRIPTION longtext DEFAULT NULL,\n"
                + "\tPLUGIN_LICENSE varchar(80) DEFAULT '' NOT NULL,\n"
                + "\tLOAD_OPTION varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_MATURITY varchar(12) DEFAULT '' NOT NULL,\n"
                + "\tPLUGIN_AUTH_VERSION varchar(80) DEFAULT NULL\n"
                + ")";

        public String PLUGIN_NAME;

        public String PLUGIN_VERSION;

        public String PLUGIN_STATUS;

        public String PLUGIN_TYPE;

        public String PLUGIN_TYPE_VERSION;

        public String PLUGIN_LIBRARY;

        public String PLUGIN_LIBRARY_VERSION;

        public String PLUGIN_AUTHOR;

        public String PLUGIN_DESCRIPTION;

        public String PLUGIN_LICENSE;

        public String LOAD_OPTION;

        public String PLUGIN_MATURITY;

        public String PLUGIN_AUTH_VERSION;
    }

    public static class USER_STATISTICS {
        public static final String createTableSQL = "CREATE TABLE `USER_STATISTICS` (\n"
                + "\tUSER varchar(128) DEFAULT '' NOT NULL,\n"
                + "\tTOTAL_CONNECTIONS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tCONCURRENT_CONNECTIONS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tCONNECTED_TIME int(11) DEFAULT 0 NOT NULL,\n"
                + "\tBUSY_TIME double DEFAULT 0 NOT NULL,\n"
                + "\tCPU_TIME double DEFAULT 0 NOT NULL,\n"
                + "\tBYTES_RECEIVED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tBYTES_SENT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tBINLOG_BYTES_WRITTEN bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_READ bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_SENT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_DELETED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_INSERTED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_UPDATED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tSELECT_COMMANDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tUPDATE_COMMANDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tOTHER_COMMANDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCOMMIT_TRANSACTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROLLBACK_TRANSACTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDENIED_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLOST_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tACCESS_DENIED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tEMPTY_QUERIES bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tTOTAL_SSL_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_STATEMENT_TIME_EXCEEDED bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String USER;

        public Integer TOTAL_CONNECTIONS;

        public Integer CONCURRENT_CONNECTIONS;

        public Integer CONNECTED_TIME;

        public Double BUSY_TIME;

        public Double CPU_TIME;

        public Long BYTES_RECEIVED;

        public Long BYTES_SENT;

        public Long BINLOG_BYTES_WRITTEN;

        public Long ROWS_READ;

        public Long ROWS_SENT;

        public Long ROWS_DELETED;

        public Long ROWS_INSERTED;

        public Long ROWS_UPDATED;

        public Long SELECT_COMMANDS;

        public Long UPDATE_COMMANDS;

        public Long OTHER_COMMANDS;

        public Long COMMIT_TRANSACTIONS;

        public Long ROLLBACK_TRANSACTIONS;

        public Long DENIED_CONNECTIONS;

        public Long LOST_CONNECTIONS;

        public Long ACCESS_DENIED;

        public Long EMPTY_QUERIES;

        public Long TOTAL_SSL_CONNECTIONS;

        public Long MAX_STATEMENT_TIME_EXCEEDED;
    }

    public static class INNODB_SYS_TABLESTATS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_TABLESTATS` (\n"
                + "\tTABLE_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tSTATS_INITIALIZED varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tNUM_ROWS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCLUST_INDEX_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tOTHER_INDEX_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMODIFIED_COUNTER bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tAUTOINC bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tREF_COUNT int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long TABLE_ID;

        public String NAME;

        public String STATS_INITIALIZED;

        public Long NUM_ROWS;

        public Long CLUST_INDEX_SIZE;

        public Long OTHER_INDEX_SIZE;

        public Long MODIFIED_COUNTER;

        public Long AUTOINC;

        public Integer REF_COUNT;
    }

    public static class INNODB_SYS_SEMAPHORE_WAITS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_SEMAPHORE_WAITS` (\n"
                + "\tTHREAD_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tOBJECT_NAME varchar(4000) DEFAULT NULL,\n"
                + "\tFILE varchar(4000) DEFAULT NULL,\n"
                + "\tLINE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tWAIT_TIME bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tWAIT_OBJECT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tWAIT_TYPE varchar(16) DEFAULT NULL,\n"
                + "\tHOLDER_THREAD_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tHOLDER_FILE varchar(4000) DEFAULT NULL,\n"
                + "\tHOLDER_LINE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tCREATED_FILE varchar(4000) DEFAULT NULL,\n"
                + "\tCREATED_LINE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tWRITER_THREAD bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tRESERVATION_MODE varchar(16) DEFAULT NULL,\n"
                + "\tREADERS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tWAITERS_FLAG bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLOCK_WORD bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLAST_WRITER_FILE varchar(4000) DEFAULT NULL,\n"
                + "\tLAST_WRITER_LINE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tOS_WAIT_COUNT int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long THREAD_ID;

        public String OBJECT_NAME;

        public String FILE;

        public Integer LINE;

        public Long WAIT_TIME;

        public Long WAIT_OBJECT;

        public String WAIT_TYPE;

        public Long HOLDER_THREAD_ID;

        public String HOLDER_FILE;

        public Integer HOLDER_LINE;

        public String CREATED_FILE;

        public Integer CREATED_LINE;

        public Long WRITER_THREAD;

        public String RESERVATION_MODE;

        public Integer READERS;

        public Long WAITERS_FLAG;

        public Long LOCK_WORD;

        public String LAST_WRITER_FILE;

        public Integer LAST_WRITER_LINE;

        public Integer OS_WAIT_COUNT;
    }

    public static class INNODB_CMP_PER_INDEX_RESET {
        public static final String createTableSQL = "CREATE TABLE `INNODB_CMP_PER_INDEX_RESET` (\n"
                + "\tdatabase_name varchar(192) DEFAULT '' NOT NULL,\n"
                + "\ttable_name varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tindex_name varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tcompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_ops_ok int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_time int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_time int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public String database_name;

        public String table_name;

        public String index_name;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    public static class TABLE_STATISTICS {
        public static final String createTableSQL = "CREATE TABLE `TABLE_STATISTICS` (\n"
                + "\tTABLE_SCHEMA varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tROWS_READ bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_CHANGED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_CHANGED_X_INDEXES bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public Long ROWS_READ;

        public Long ROWS_CHANGED;

        public Long ROWS_CHANGED_X_INDEXES;
    }

    public static class COLLATION_CHARACTER_SET_APPLICABILITY {
        public static final String createTableSQL = "CREATE TABLE `COLLATION_CHARACTER_SET_APPLICABILITY` (\n"
                + "\tCOLLATION_NAME varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_SET_NAME varchar(32) DEFAULT '' NOT NULL\n"
                + ")";

        public String COLLATION_NAME;

        public String CHARACTER_SET_NAME;
    }

    public static class ENGINES {
        public static final String createTableSQL = "CREATE TABLE `ENGINES` (\n"
                + "\tENGINE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tSUPPORT varchar(8) DEFAULT '' NOT NULL,\n"
                + "\tCOMMENT varchar(160) DEFAULT '' NOT NULL,\n"
                + "\tTRANSACTIONS varchar(3) DEFAULT NULL,\n"
                + "\tXA varchar(3) DEFAULT NULL,\n"
                + "\tSAVEPOINTS varchar(3) DEFAULT NULL\n"
                + ")";

        public String ENGINE;

        public String SUPPORT;

        public String COMMENT;

        public String TRANSACTIONS;

        public String XA;

        public String SAVEPOINTS;
    }

    public static class KEY_COLUMN_USAGE {
        public static final String createTableSQL = "CREATE TABLE `KEY_COLUMN_USAGE` (\n"
                + "\tCONSTRAINT_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCOLUMN_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tORDINAL_POSITION bigint(10) DEFAULT 0 NOT NULL,\n"
                + "\tPOSITION_IN_UNIQUE_CONSTRAINT bigint(10) DEFAULT NULL,\n"
                + "\tREFERENCED_TABLE_SCHEMA varchar(64) DEFAULT NULL,\n"
                + "\tREFERENCED_TABLE_NAME varchar(64) DEFAULT NULL,\n"
                + "\tREFERENCED_COLUMN_NAME varchar(64) DEFAULT NULL\n"
                + ")";

        public String CONSTRAINT_CATALOG;

        public String CONSTRAINT_SCHEMA;

        public String CONSTRAINT_NAME;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String COLUMN_NAME;

        public Long ORDINAL_POSITION;

        public Long POSITION_IN_UNIQUE_CONSTRAINT;

        public String REFERENCED_TABLE_SCHEMA;

        public String REFERENCED_TABLE_NAME;

        public String REFERENCED_COLUMN_NAME;
    }

    public static class GLOBAL_VARIABLES {
        public static final String createTableSQL = "CREATE TABLE `GLOBAL_VARIABLES` (\n"
                + "\tVARIABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVARIABLE_VALUE varchar(2048) DEFAULT '' NOT NULL\n"
                + ")";

        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    public static class INNODB_LOCKS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_LOCKS` (\n"
                + "\tlock_id varchar(81) DEFAULT '' NOT NULL,\n"
                + "\tlock_trx_id varchar(18) DEFAULT '' NOT NULL,\n"
                + "\tlock_mode varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tlock_type varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tlock_table varchar(1024) DEFAULT '' NOT NULL,\n"
                + "\tlock_index varchar(1024) DEFAULT NULL,\n"
                + "\tlock_space bigint(21) DEFAULT NULL,\n"
                + "\tlock_page bigint(21) DEFAULT NULL,\n"
                + "\tlock_rec bigint(21) DEFAULT NULL,\n"
                + "\tlock_data varchar(8192) DEFAULT NULL\n"
                + ")";

        public String lock_id;

        public String lock_trx_id;

        public String lock_mode;

        public String lock_type;

        public String lock_table;

        public String lock_index;

        public Long lock_space;

        public Long lock_page;

        public Long lock_rec;

        public String lock_data;
    }

    public static class INNODB_SYS_FOREIGN_COLS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_FOREIGN_COLS` (\n"
                + "\tID varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tFOR_COL_NAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tREF_COL_NAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tPOS int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public String ID;

        public String FOR_COL_NAME;

        public String REF_COL_NAME;

        public Integer POS;
    }

    public static class INNODB_BUFFER_PAGE {
        public static final String createTableSQL = "CREATE TABLE `INNODB_BUFFER_PAGE` (\n"
                + "\tPOOL_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tBLOCK_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tSPACE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGE_NUMBER bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGE_TYPE varchar(64) DEFAULT NULL,\n"
                + "\tFLUSH_TYPE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tFIX_COUNT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tIS_HASHED varchar(3) DEFAULT NULL,\n"
                + "\tNEWEST_MODIFICATION bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tOLDEST_MODIFICATION bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tACCESS_TIME bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tTABLE_NAME varchar(1024) DEFAULT NULL,\n"
                + "\tINDEX_NAME varchar(1024) DEFAULT NULL,\n"
                + "\tNUMBER_RECORDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDATA_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCOMPRESSED_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGE_STATE varchar(64) DEFAULT NULL,\n"
                + "\tIO_FIX varchar(64) DEFAULT NULL,\n"
                + "\tIS_OLD varchar(3) DEFAULT NULL,\n"
                + "\tFREE_PAGE_CLOCK bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long POOL_ID;

        public Long BLOCK_ID;

        public Long SPACE;

        public Long PAGE_NUMBER;

        public String PAGE_TYPE;

        public Long FLUSH_TYPE;

        public Long FIX_COUNT;

        public String IS_HASHED;

        public Long NEWEST_MODIFICATION;

        public Long OLDEST_MODIFICATION;

        public Long ACCESS_TIME;

        public String TABLE_NAME;

        public String INDEX_NAME;

        public Long NUMBER_RECORDS;

        public Long DATA_SIZE;

        public Long COMPRESSED_SIZE;

        public String PAGE_STATE;

        public String IO_FIX;

        public String IS_OLD;

        public Long FREE_PAGE_CLOCK;
    }

    public static class SESSION_STATUS {
        public static final String createTableSQL = "CREATE TABLE `SESSION_STATUS` (\n"
                + "\tVARIABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVARIABLE_VALUE varchar(2048) DEFAULT '' NOT NULL\n"
                + ")";

        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    public static class INNODB_SYS_DATAFILES {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_DATAFILES` (\n"
                + "\tSPACE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tPATH varchar(4000) DEFAULT '' NOT NULL\n"
                + ")";

        public Integer SPACE;

        public String PATH;
    }

    public static class INNODB_CMP {
        public static final String createTableSQL = "CREATE TABLE `INNODB_CMP` (\n"
                + "\tpage_size int(5) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_ops_ok int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_time int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_time int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Integer page_size;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    public static class KEY_CACHES {
        public static final String createTableSQL = "CREATE TABLE `KEY_CACHES` (\n"
                + "\tKEY_CACHE_NAME varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tSEGMENTS int(3) DEFAULT NULL,\n"
                + "\tSEGMENT_NUMBER int(3) DEFAULT NULL,\n"
                + "\tFULL_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tBLOCK_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tUSED_BLOCKS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tUNUSED_BLOCKS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDIRTY_BLOCKS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tREAD_REQUESTS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tREADS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tWRITE_REQUESTS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tWRITES bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String KEY_CACHE_NAME;

        public Integer SEGMENTS;

        public Integer SEGMENT_NUMBER;

        public Long FULL_SIZE;

        public Long BLOCK_SIZE;

        public Long USED_BLOCKS;

        public Long UNUSED_BLOCKS;

        public Long DIRTY_BLOCKS;

        public Long READ_REQUESTS;

        public Long READS;

        public Long WRITE_REQUESTS;

        public Long WRITES;
    }

    public static class INNODB_CMPMEM_RESET {
        public static final String createTableSQL = "CREATE TABLE `INNODB_CMPMEM_RESET` (\n"
                + "\tpage_size int(5) DEFAULT 0 NOT NULL,\n"
                + "\tbuffer_pool_instance int(11) DEFAULT 0 NOT NULL,\n"
                + "\tpages_used int(11) DEFAULT 0 NOT NULL,\n"
                + "\tpages_free int(11) DEFAULT 0 NOT NULL,\n"
                + "\trelocation_ops bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\trelocation_time int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Integer page_size;

        public Integer buffer_pool_instance;

        public Integer pages_used;

        public Integer pages_free;

        public Long relocation_ops;

        public Integer relocation_time;
    }

    public static class INNODB_SYS_VIRTUAL {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_VIRTUAL` (\n"
                + "\tTABLE_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPOS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tBASE_POS int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long TABLE_ID;

        public Integer POS;

        public Integer BASE_POS;
    }

    public static class APPLICABLE_ROLES {
        public static final String createTableSQL = "CREATE TABLE `APPLICABLE_ROLES` (\n"
                + "\tGRANTEE varchar(190) DEFAULT '' NOT NULL,\n"
                + "\tROLE_NAME varchar(128) DEFAULT '' NOT NULL,\n"
                + "\tIS_GRANTABLE varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tIS_DEFAULT varchar(3) DEFAULT NULL\n"
                + ")";

        public String GRANTEE;

        public String ROLE_NAME;

        public String IS_GRANTABLE;

        public String IS_DEFAULT;
    }

    public static class INNODB_SYS_FIELDS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_FIELDS` (\n"
                + "\tINDEX_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tPOS int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long INDEX_ID;

        public String NAME;

        public Integer POS;
    }

    public static class TABLESPACES {
        public static final String createTableSQL = "CREATE TABLE `TABLESPACES` (\n"
                + "\tTABLESPACE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tENGINE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLESPACE_TYPE varchar(64) DEFAULT NULL,\n"
                + "\tLOGFILE_GROUP_NAME varchar(64) DEFAULT NULL,\n"
                + "\tEXTENT_SIZE bigint(21) DEFAULT NULL,\n"
                + "\tAUTOEXTEND_SIZE bigint(21) DEFAULT NULL,\n"
                + "\tMAXIMUM_SIZE bigint(21) DEFAULT NULL,\n"
                + "\tNODEGROUP_ID bigint(21) DEFAULT NULL,\n"
                + "\tTABLESPACE_COMMENT varchar(2048) DEFAULT NULL\n"
                + ")";

        public String TABLESPACE_NAME;

        public String ENGINE;

        public String TABLESPACE_TYPE;

        public String LOGFILE_GROUP_NAME;

        public Long EXTENT_SIZE;

        public Long AUTOEXTEND_SIZE;

        public Long MAXIMUM_SIZE;

        public Long NODEGROUP_ID;

        public String TABLESPACE_COMMENT;
    }

    public static class REFERENTIAL_CONSTRAINTS {
        public static final String createTableSQL = "CREATE TABLE `REFERENTIAL_CONSTRAINTS` (\n"
                + "\tCONSTRAINT_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tUNIQUE_CONSTRAINT_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tUNIQUE_CONSTRAINT_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tUNIQUE_CONSTRAINT_NAME varchar(64) DEFAULT NULL,\n"
                + "\tMATCH_OPTION varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tUPDATE_RULE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tDELETE_RULE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tREFERENCED_TABLE_NAME varchar(64) DEFAULT '' NOT NULL\n"
                + ")";

        public String CONSTRAINT_CATALOG;

        public String CONSTRAINT_SCHEMA;

        public String CONSTRAINT_NAME;

        public String UNIQUE_CONSTRAINT_CATALOG;

        public String UNIQUE_CONSTRAINT_SCHEMA;

        public String UNIQUE_CONSTRAINT_NAME;

        public String MATCH_OPTION;

        public String UPDATE_RULE;

        public String DELETE_RULE;

        public String TABLE_NAME;

        public String REFERENCED_TABLE_NAME;
    }

    public static class INNODB_SYS_TABLES {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_TABLES` (\n"
                + "\tTABLE_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(655) DEFAULT '' NOT NULL,\n"
                + "\tFLAG int(11) DEFAULT 0 NOT NULL,\n"
                + "\tN_COLS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tSPACE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tROW_FORMAT varchar(12) DEFAULT NULL,\n"
                + "\tZIP_PAGE_SIZE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tSPACE_TYPE varchar(10) DEFAULT NULL\n"
                + ")";

        public Long TABLE_ID;

        public String NAME;

        public Integer FLAG;

        public Integer N_COLS;

        public Integer SPACE;

        public String ROW_FORMAT;

        public Integer ZIP_PAGE_SIZE;

        public String SPACE_TYPE;
    }

    public static class SCHEMATA {
        public static final String createTableSQL = "CREATE TABLE `SCHEMATA` (\n"
                + "\tCATALOG_NAME varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tSCHEMA_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tDEFAULT_CHARACTER_SET_NAME varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tDEFAULT_COLLATION_NAME varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tSQL_PATH varchar(512) DEFAULT NULL\n"
                + ")";

        public String CATALOG_NAME;

        public String SCHEMA_NAME;

        public String DEFAULT_CHARACTER_SET_NAME;

        public String DEFAULT_COLLATION_NAME;

        public String SQL_PATH;
    }

    public static class INNODB_FT_BEING_DELETED {
        public static final String createTableSQL = "CREATE TABLE `INNODB_FT_BEING_DELETED` (\n"
                + "\tDOC_ID bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long DOC_ID;
    }

    public static class ENABLED_ROLES {
        public static final String createTableSQL = "CREATE TABLE `ENABLED_ROLES` (\n"
                + "\tROLE_NAME varchar(128) DEFAULT NULL\n"
                + ")";

        public String ROLE_NAME;
    }

    public static class COLUMNS {
        public static final String createTableSQL = "CREATE TABLE `COLUMNS` (\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCOLUMN_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tORDINAL_POSITION bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCOLUMN_DEFAULT longtext DEFAULT NULL,\n"
                + "\tIS_NULLABLE varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tDATA_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_MAXIMUM_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tCHARACTER_OCTET_LENGTH bigint(21) DEFAULT NULL,\n"
                + "\tNUMERIC_PRECISION bigint(21) DEFAULT NULL,\n"
                + "\tNUMERIC_SCALE bigint(21) DEFAULT NULL,\n"
                + "\tDATETIME_PRECISION bigint(21) DEFAULT NULL,\n"
                + "\tCHARACTER_SET_NAME varchar(32) DEFAULT NULL,\n"
                + "\tCOLLATION_NAME varchar(32) DEFAULT NULL,\n"
                + "\tCOLUMN_TYPE longtext DEFAULT '' NOT NULL,\n"
                + "\tCOLUMN_KEY varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tEXTRA varchar(30) DEFAULT '' NOT NULL,\n"
                + "\tPRIVILEGES varchar(80) DEFAULT '' NOT NULL,\n"
                + "\tCOLUMN_COMMENT varchar(1024) DEFAULT '' NOT NULL,\n"
                + "\tIS_GENERATED varchar(6) DEFAULT '' NOT NULL,\n"
                + "\tGENERATION_EXPRESSION longtext DEFAULT NULL\n"
                + ")";

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String COLUMN_NAME;

        public Long ORDINAL_POSITION;

        public String COLUMN_DEFAULT;

        public String IS_NULLABLE;

        public String DATA_TYPE;

        public Long CHARACTER_MAXIMUM_LENGTH;

        public Long CHARACTER_OCTET_LENGTH;

        public Long NUMERIC_PRECISION;

        public Long NUMERIC_SCALE;

        public Long DATETIME_PRECISION;

        public String CHARACTER_SET_NAME;

        public String COLLATION_NAME;

        public String COLUMN_TYPE;

        public String COLUMN_KEY;

        public String EXTRA;

        public String PRIVILEGES;

        public String COLUMN_COMMENT;

        public String IS_GENERATED;

        public String GENERATION_EXPRESSION;
    }

    public static class INNODB_BUFFER_POOL_STATS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_BUFFER_POOL_STATS` (\n"
                + "\tPOOL_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPOOL_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tFREE_BUFFERS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDATABASE_PAGES bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tOLD_DATABASE_PAGES bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMODIFIED_DATABASE_PAGES bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPENDING_DECOMPRESS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPENDING_READS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPENDING_FLUSH_LRU bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPENDING_FLUSH_LIST bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGES_MADE_YOUNG bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGES_NOT_MADE_YOUNG bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGES_MADE_YOUNG_RATE double DEFAULT 0 NOT NULL,\n"
                + "\tPAGES_MADE_NOT_YOUNG_RATE double DEFAULT 0 NOT NULL,\n"
                + "\tNUMBER_PAGES_READ bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNUMBER_PAGES_CREATED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNUMBER_PAGES_WRITTEN bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGES_READ_RATE double DEFAULT 0 NOT NULL,\n"
                + "\tPAGES_CREATE_RATE double DEFAULT 0 NOT NULL,\n"
                + "\tPAGES_WRITTEN_RATE double DEFAULT 0 NOT NULL,\n"
                + "\tNUMBER_PAGES_GET bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tHIT_RATE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tYOUNG_MAKE_PER_THOUSAND_GETS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNOT_YOUNG_MAKE_PER_THOUSAND_GETS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNUMBER_PAGES_READ_AHEAD bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNUMBER_READ_AHEAD_EVICTED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tREAD_AHEAD_RATE double DEFAULT 0 NOT NULL,\n"
                + "\tREAD_AHEAD_EVICTED_RATE double DEFAULT 0 NOT NULL,\n"
                + "\tLRU_IO_TOTAL bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLRU_IO_CURRENT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tUNCOMPRESS_TOTAL bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tUNCOMPRESS_CURRENT bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long POOL_ID;

        public Long POOL_SIZE;

        public Long FREE_BUFFERS;

        public Long DATABASE_PAGES;

        public Long OLD_DATABASE_PAGES;

        public Long MODIFIED_DATABASE_PAGES;

        public Long PENDING_DECOMPRESS;

        public Long PENDING_READS;

        public Long PENDING_FLUSH_LRU;

        public Long PENDING_FLUSH_LIST;

        public Long PAGES_MADE_YOUNG;

        public Long PAGES_NOT_MADE_YOUNG;

        public Double PAGES_MADE_YOUNG_RATE;

        public Double PAGES_MADE_NOT_YOUNG_RATE;

        public Long NUMBER_PAGES_READ;

        public Long NUMBER_PAGES_CREATED;

        public Long NUMBER_PAGES_WRITTEN;

        public Double PAGES_READ_RATE;

        public Double PAGES_CREATE_RATE;

        public Double PAGES_WRITTEN_RATE;

        public Long NUMBER_PAGES_GET;

        public Long HIT_RATE;

        public Long YOUNG_MAKE_PER_THOUSAND_GETS;

        public Long NOT_YOUNG_MAKE_PER_THOUSAND_GETS;

        public Long NUMBER_PAGES_READ_AHEAD;

        public Long NUMBER_READ_AHEAD_EVICTED;

        public Double READ_AHEAD_RATE;

        public Double READ_AHEAD_EVICTED_RATE;

        public Long LRU_IO_TOTAL;

        public Long LRU_IO_CURRENT;

        public Long UNCOMPRESS_TOTAL;

        public Long UNCOMPRESS_CURRENT;
    }

    public static class INNODB_CMP_PER_INDEX {
        public static final String createTableSQL = "CREATE TABLE `INNODB_CMP_PER_INDEX` (\n"
                + "\tdatabase_name varchar(192) DEFAULT '' NOT NULL,\n"
                + "\ttable_name varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tindex_name varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tcompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_ops_ok int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_time int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_time int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public String database_name;

        public String table_name;

        public String index_name;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    public static class INNODB_MUTEXES {
        public static final String createTableSQL = "CREATE TABLE `INNODB_MUTEXES` (\n"
                + "\tNAME varchar(4000) DEFAULT '' NOT NULL,\n"
                + "\tCREATE_FILE varchar(4000) DEFAULT '' NOT NULL,\n"
                + "\tCREATE_LINE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tOS_WAITS bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String NAME;

        public String CREATE_FILE;

        public Integer CREATE_LINE;

        public Long OS_WAITS;
    }

    public static class INNODB_BUFFER_PAGE_LRU {
        public static final String createTableSQL = "CREATE TABLE `INNODB_BUFFER_PAGE_LRU` (\n"
                + "\tPOOL_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLRU_POSITION bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tSPACE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGE_NUMBER bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPAGE_TYPE varchar(64) DEFAULT NULL,\n"
                + "\tFLUSH_TYPE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tFIX_COUNT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tIS_HASHED varchar(3) DEFAULT NULL,\n"
                + "\tNEWEST_MODIFICATION bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tOLDEST_MODIFICATION bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tACCESS_TIME bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tTABLE_NAME varchar(1024) DEFAULT NULL,\n"
                + "\tINDEX_NAME varchar(1024) DEFAULT NULL,\n"
                + "\tNUMBER_RECORDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDATA_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCOMPRESSED_SIZE bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCOMPRESSED varchar(3) DEFAULT NULL,\n"
                + "\tIO_FIX varchar(64) DEFAULT NULL,\n"
                + "\tIS_OLD varchar(3) DEFAULT NULL,\n"
                + "\tFREE_PAGE_CLOCK bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long POOL_ID;

        public Long LRU_POSITION;

        public Long SPACE;

        public Long PAGE_NUMBER;

        public String PAGE_TYPE;

        public Long FLUSH_TYPE;

        public Long FIX_COUNT;

        public String IS_HASHED;

        public Long NEWEST_MODIFICATION;

        public Long OLDEST_MODIFICATION;

        public Long ACCESS_TIME;

        public String TABLE_NAME;

        public String INDEX_NAME;

        public Long NUMBER_RECORDS;

        public Long DATA_SIZE;

        public Long COMPRESSED_SIZE;

        public String COMPRESSED;

        public String IO_FIX;

        public String IS_OLD;

        public Long FREE_PAGE_CLOCK;
    }

    public static class INNODB_FT_CONFIG {
        public static final String createTableSQL = "CREATE TABLE `INNODB_FT_CONFIG` (\n"
                + "\tKEY varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tVALUE varchar(193) DEFAULT '' NOT NULL\n"
                + ")";

        public String KEY;

        public String VALUE;
    }

    public static class SYSTEM_VARIABLES {
        public static final String createTableSQL = "CREATE TABLE `SYSTEM_VARIABLES` (\n"
                + "\tVARIABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tSESSION_VALUE varchar(2048) DEFAULT NULL,\n"
                + "\tGLOBAL_VALUE varchar(2048) DEFAULT NULL,\n"
                + "\tGLOBAL_VALUE_ORIGIN varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tDEFAULT_VALUE varchar(2048) DEFAULT NULL,\n"
                + "\tVARIABLE_SCOPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVARIABLE_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVARIABLE_COMMENT varchar(2048) DEFAULT '' NOT NULL,\n"
                + "\tNUMERIC_MIN_VALUE varchar(21) DEFAULT NULL,\n"
                + "\tNUMERIC_MAX_VALUE varchar(21) DEFAULT NULL,\n"
                + "\tNUMERIC_BLOCK_SIZE varchar(21) DEFAULT NULL,\n"
                + "\tENUM_VALUE_LIST longtext DEFAULT NULL,\n"
                + "\tREAD_ONLY varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tCOMMAND_LINE_ARGUMENT varchar(64) DEFAULT NULL\n"
                + ")";

        public String VARIABLE_NAME;

        public String SESSION_VALUE;

        public String GLOBAL_VALUE;

        public String GLOBAL_VALUE_ORIGIN;

        public String DEFAULT_VALUE;

        public String VARIABLE_SCOPE;

        public String VARIABLE_TYPE;

        public String VARIABLE_COMMENT;

        public String NUMERIC_MIN_VALUE;

        public String NUMERIC_MAX_VALUE;

        public String NUMERIC_BLOCK_SIZE;

        public String ENUM_VALUE_LIST;

        public String READ_ONLY;

        public String COMMAND_LINE_ARGUMENT;
    }

    public static class TABLE_CONSTRAINTS {
        public static final String createTableSQL = "CREATE TABLE `TABLE_CONSTRAINTS` (\n"
                + "\tCONSTRAINT_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCONSTRAINT_TYPE varchar(64) DEFAULT '' NOT NULL\n"
                + ")";

        public String CONSTRAINT_CATALOG;

        public String CONSTRAINT_SCHEMA;

        public String CONSTRAINT_NAME;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String CONSTRAINT_TYPE;
    }

    public static class CLIENT_STATISTICS {
        public static final String createTableSQL = "CREATE TABLE `CLIENT_STATISTICS` (\n"
                + "\tCLIENT varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTOTAL_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCONCURRENT_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCONNECTED_TIME bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tBUSY_TIME double DEFAULT 0 NOT NULL,\n"
                + "\tCPU_TIME double DEFAULT 0 NOT NULL,\n"
                + "\tBYTES_RECEIVED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tBYTES_SENT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tBINLOG_BYTES_WRITTEN bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_READ bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_SENT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_DELETED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_INSERTED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROWS_UPDATED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tSELECT_COMMANDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tUPDATE_COMMANDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tOTHER_COMMANDS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tCOMMIT_TRANSACTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tROLLBACK_TRANSACTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDENIED_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLOST_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tACCESS_DENIED bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tEMPTY_QUERIES bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tTOTAL_SSL_CONNECTIONS bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_STATEMENT_TIME_EXCEEDED bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String CLIENT;

        public Long TOTAL_CONNECTIONS;

        public Long CONCURRENT_CONNECTIONS;

        public Long CONNECTED_TIME;

        public Double BUSY_TIME;

        public Double CPU_TIME;

        public Long BYTES_RECEIVED;

        public Long BYTES_SENT;

        public Long BINLOG_BYTES_WRITTEN;

        public Long ROWS_READ;

        public Long ROWS_SENT;

        public Long ROWS_DELETED;

        public Long ROWS_INSERTED;

        public Long ROWS_UPDATED;

        public Long SELECT_COMMANDS;

        public Long UPDATE_COMMANDS;

        public Long OTHER_COMMANDS;

        public Long COMMIT_TRANSACTIONS;

        public Long ROLLBACK_TRANSACTIONS;

        public Long DENIED_CONNECTIONS;

        public Long LOST_CONNECTIONS;

        public Long ACCESS_DENIED;

        public Long EMPTY_QUERIES;

        public Long TOTAL_SSL_CONNECTIONS;

        public Long MAX_STATEMENT_TIME_EXCEEDED;
    }

    public static class PROFILING {
        public static final String createTableSQL = "CREATE TABLE `PROFILING` (\n"
                + "\tQUERY_ID int(20) DEFAULT 0 NOT NULL,\n"
                + "\tSEQ int(20) DEFAULT 0 NOT NULL,\n"
                + "\tSTATE varchar(30) DEFAULT '' NOT NULL,\n"
                + "\tDURATION decimal(9, 6) DEFAULT 0.000000 NOT NULL,\n"
                + "\tCPU_USER decimal(9, 6) DEFAULT NULL,\n"
                + "\tCPU_SYSTEM decimal(9, 6) DEFAULT NULL,\n"
                + "\tCONTEXT_VOLUNTARY int(20) DEFAULT NULL,\n"
                + "\tCONTEXT_INVOLUNTARY int(20) DEFAULT NULL,\n"
                + "\tBLOCK_OPS_IN int(20) DEFAULT NULL,\n"
                + "\tBLOCK_OPS_OUT int(20) DEFAULT NULL,\n"
                + "\tMESSAGES_SENT int(20) DEFAULT NULL,\n"
                + "\tMESSAGES_RECEIVED int(20) DEFAULT NULL,\n"
                + "\tPAGE_FAULTS_MAJOR int(20) DEFAULT NULL,\n"
                + "\tPAGE_FAULTS_MINOR int(20) DEFAULT NULL,\n"
                + "\tSWAPS int(20) DEFAULT NULL,\n"
                + "\tSOURCE_FUNCTION varchar(30) DEFAULT NULL,\n"
                + "\tSOURCE_FILE varchar(20) DEFAULT NULL,\n"
                + "\tSOURCE_LINE int(20) DEFAULT NULL\n"
                + ")";

        public Integer QUERY_ID;

        public Integer SEQ;

        public String STATE;

        public BigDecimal DURATION;

        public BigDecimal CPU_USER;

        public BigDecimal CPU_SYSTEM;

        public Integer CONTEXT_VOLUNTARY;

        public Integer CONTEXT_INVOLUNTARY;

        public Integer BLOCK_OPS_IN;

        public Integer BLOCK_OPS_OUT;

        public Integer MESSAGES_SENT;

        public Integer MESSAGES_RECEIVED;

        public Integer PAGE_FAULTS_MAJOR;

        public Integer PAGE_FAULTS_MINOR;

        public Integer SWAPS;

        public String SOURCE_FUNCTION;

        public String SOURCE_FILE;

        public Integer SOURCE_LINE;
    }

    public static class INNODB_TABLESPACES_ENCRYPTION {
        public static final String createTableSQL = "CREATE TABLE `INNODB_TABLESPACES_ENCRYPTION` (\n"
                + "\tSPACE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(655) DEFAULT NULL,\n"
                + "\tENCRYPTION_SCHEME int(11) DEFAULT 0 NOT NULL,\n"
                + "\tKEYSERVER_REQUESTS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tMIN_KEY_VERSION int(11) DEFAULT 0 NOT NULL,\n"
                + "\tCURRENT_KEY_VERSION int(11) DEFAULT 0 NOT NULL,\n"
                + "\tKEY_ROTATION_PAGE_NUMBER bigint(21) DEFAULT NULL,\n"
                + "\tKEY_ROTATION_MAX_PAGE_NUMBER bigint(21) DEFAULT NULL,\n"
                + "\tCURRENT_KEY_ID int(11) DEFAULT 0 NOT NULL,\n"
                + "\tROTATING_OR_FLUSHING int(1) DEFAULT 0 NOT NULL\n"
                + ")";

        public Integer SPACE;

        public String NAME;

        public Integer ENCRYPTION_SCHEME;

        public Integer KEYSERVER_REQUESTS;

        public Integer MIN_KEY_VERSION;

        public Integer CURRENT_KEY_VERSION;

        public Long KEY_ROTATION_PAGE_NUMBER;

        public Long KEY_ROTATION_MAX_PAGE_NUMBER;

        public Integer CURRENT_KEY_ID;

        public Integer ROTATING_OR_FLUSHING;
    }

    public static class INNODB_SYS_FOREIGN {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_FOREIGN` (\n"
                + "\tID varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tFOR_NAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tREF_NAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tN_COLS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tTYPE int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public String ID;

        public String FOR_NAME;

        public String REF_NAME;

        public Integer N_COLS;

        public Integer TYPE;
    }

    public static class COLLATIONS {
        public static final String createTableSQL = "CREATE TABLE `COLLATIONS` (\n"
                + "\tCOLLATION_NAME varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_SET_NAME varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tID bigint(11) DEFAULT 0 NOT NULL,\n"
                + "\tIS_DEFAULT varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tIS_COMPILED varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tSORTLEN bigint(3) DEFAULT 0 NOT NULL\n"
                + ")";

        public String COLLATION_NAME;

        public String CHARACTER_SET_NAME;

        public Long ID;

        public String IS_DEFAULT;

        public String IS_COMPILED;

        public Long SORTLEN;
    }

    public static class INNODB_CMPMEM {
        public static final String createTableSQL = "CREATE TABLE `INNODB_CMPMEM` (\n"
                + "\tpage_size int(5) DEFAULT 0 NOT NULL,\n"
                + "\tbuffer_pool_instance int(11) DEFAULT 0 NOT NULL,\n"
                + "\tpages_used int(11) DEFAULT 0 NOT NULL,\n"
                + "\tpages_free int(11) DEFAULT 0 NOT NULL,\n"
                + "\trelocation_ops bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\trelocation_time int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Integer page_size;

        public Integer buffer_pool_instance;

        public Integer pages_used;

        public Integer pages_free;

        public Long relocation_ops;

        public Integer relocation_time;
    }

    public static class INNODB_TRX {
        public static final String createTableSQL = "CREATE TABLE `INNODB_TRX` (\n"
                + "\ttrx_id varchar(18) DEFAULT '' NOT NULL,\n"
                + "\ttrx_state varchar(13) DEFAULT '' NOT NULL,\n"
                + "\ttrx_started datetime DEFAULT '0000-00-00 00:00:00' NOT NULL,\n"
                + "\ttrx_requested_lock_id varchar(81) DEFAULT NULL,\n"
                + "\ttrx_wait_started datetime DEFAULT NULL,\n"
                + "\ttrx_weight bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_mysql_thread_id bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_query varchar(1024) DEFAULT NULL,\n"
                + "\ttrx_operation_state varchar(64) DEFAULT NULL,\n"
                + "\ttrx_tables_in_use bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_tables_locked bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_lock_structs bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_lock_memory_bytes bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_rows_locked bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_rows_modified bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_concurrency_tickets bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_isolation_level varchar(16) DEFAULT '' NOT NULL,\n"
                + "\ttrx_unique_checks int(1) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_foreign_key_checks int(1) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_last_foreign_key_error varchar(256) DEFAULT NULL,\n"
                + "\ttrx_is_read_only int(1) DEFAULT 0 NOT NULL,\n"
                + "\ttrx_autocommit_non_locking int(1) DEFAULT 0 NOT NULL\n"
                + ")";

        public String trx_id;

        public String trx_state;

        public Long trx_started;

        public String trx_requested_lock_id;

        public Long trx_wait_started;

        public Long trx_weight;

        public Long trx_mysql_thread_id;

        public String trx_query;

        public String trx_operation_state;

        public Long trx_tables_in_use;

        public Long trx_tables_locked;

        public Long trx_lock_structs;

        public Long trx_lock_memory_bytes;

        public Long trx_rows_locked;

        public Long trx_rows_modified;

        public Long trx_concurrency_tickets;

        public String trx_isolation_level;

        public Integer trx_unique_checks;

        public Integer trx_foreign_key_checks;

        public String trx_last_foreign_key_error;

        public Integer trx_is_read_only;

        public Integer trx_autocommit_non_locking;
    }

    public static class CHARACTER_SETS {
        public static final String createTableSQL = "CREATE TABLE `CHARACTER_SETS` (\n"
                + "\tCHARACTER_SET_NAME varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tDEFAULT_COLLATE_NAME varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tDESCRIPTION varchar(60) DEFAULT '' NOT NULL,\n"
                + "\tMAXLEN bigint(3) DEFAULT 0 NOT NULL\n"
                + ")";

        public String CHARACTER_SET_NAME;

        public String DEFAULT_COLLATE_NAME;

        public String DESCRIPTION;

        public Long MAXLEN;
    }

    public static class INDEX_STATISTICS {
        public static final String createTableSQL = "CREATE TABLE `INDEX_STATISTICS` (\n"
                + "\tTABLE_SCHEMA varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tINDEX_NAME varchar(192) DEFAULT '' NOT NULL,\n"
                + "\tROWS_READ bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String INDEX_NAME;

        public Long ROWS_READ;
    }

    public static class INNODB_FT_DELETED {
        public static final String createTableSQL = "CREATE TABLE `INNODB_FT_DELETED` (\n"
                + "\tDOC_ID bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long DOC_ID;
    }

    public static class STATISTICS {
        public static final String createTableSQL = "CREATE TABLE `STATISTICS` (\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tNON_UNIQUE bigint(1) DEFAULT 0 NOT NULL,\n"
                + "\tINDEX_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tINDEX_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tSEQ_IN_INDEX bigint(2) DEFAULT 0 NOT NULL,\n"
                + "\tCOLUMN_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCOLLATION varchar(1) DEFAULT NULL,\n"
                + "\tCARDINALITY bigint(21) DEFAULT NULL,\n"
                + "\tSUB_PART bigint(3) DEFAULT NULL,\n"
                + "\tPACKED varchar(10) DEFAULT NULL,\n"
                + "\tNULLABLE varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tINDEX_TYPE varchar(16) DEFAULT '' NOT NULL,\n"
                + "\tCOMMENT varchar(16) DEFAULT NULL,\n"
                + "\tINDEX_COMMENT varchar(1024) DEFAULT '' NOT NULL\n"
                + ")";

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public Long NON_UNIQUE;

        public String INDEX_SCHEMA;

        public String INDEX_NAME;

        public Long SEQ_IN_INDEX;

        public String COLUMN_NAME;

        public String COLLATION;

        public Long CARDINALITY;

        public Long SUB_PART;

        public String PACKED;

        public String NULLABLE;

        public String INDEX_TYPE;

        public String COMMENT;

        public String INDEX_COMMENT;
    }

    public static class VIEWS {
        public static final String createTableSQL = "CREATE TABLE `VIEWS` (\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVIEW_DEFINITION longtext DEFAULT '' NOT NULL,\n"
                + "\tCHECK_OPTION varchar(8) DEFAULT '' NOT NULL,\n"
                + "\tIS_UPDATABLE varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tDEFINER varchar(189) DEFAULT '' NOT NULL,\n"
                + "\tSECURITY_TYPE varchar(7) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_SET_CLIENT varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tCOLLATION_CONNECTION varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tALGORITHM varchar(10) DEFAULT '' NOT NULL\n"
                + ")";

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String VIEW_DEFINITION;

        public String CHECK_OPTION;

        public String IS_UPDATABLE;

        public String DEFINER;

        public String SECURITY_TYPE;

        public String CHARACTER_SET_CLIENT;

        public String COLLATION_CONNECTION;

        public String ALGORITHM;
    }

    public static class COLUMN_PRIVILEGES {
        public static final String createTableSQL = "CREATE TABLE `COLUMN_PRIVILEGES` (\n"
                + "\tGRANTEE varchar(190) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCOLUMN_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPRIVILEGE_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tIS_GRANTABLE varchar(3) DEFAULT '' NOT NULL\n"
                + ")";

        public String GRANTEE;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String COLUMN_NAME;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    public static class user_variables {
        public static final String createTableSQL = "CREATE TABLE `user_variables` (\n"
                + "\tVARIABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVARIABLE_VALUE varchar(2048) DEFAULT NULL,\n"
                + "\tVARIABLE_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_SET_NAME varchar(32) DEFAULT NULL\n"
                + ")";

        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;

        public String VARIABLE_TYPE;

        public String CHARACTER_SET_NAME;
    }

    public static class SESSION_VARIABLES {
        public static final String createTableSQL = "CREATE TABLE `SESSION_VARIABLES` (\n"
                + "\tVARIABLE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tVARIABLE_VALUE varchar(2048) DEFAULT '' NOT NULL\n"
                + ")";

        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    public static class INNODB_METRICS {
        public static final String createTableSQL = "CREATE TABLE `INNODB_METRICS` (\n"
                + "\tNAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tSUBSYSTEM varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tCOUNT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_COUNT bigint(21) DEFAULT NULL,\n"
                + "\tMIN_COUNT bigint(21) DEFAULT NULL,\n"
                + "\tAVG_COUNT double DEFAULT NULL,\n"
                + "\tCOUNT_RESET bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_COUNT_RESET bigint(21) DEFAULT NULL,\n"
                + "\tMIN_COUNT_RESET bigint(21) DEFAULT NULL,\n"
                + "\tAVG_COUNT_RESET double DEFAULT NULL,\n"
                + "\tTIME_ENABLED datetime DEFAULT NULL,\n"
                + "\tTIME_DISABLED datetime DEFAULT NULL,\n"
                + "\tTIME_ELAPSED bigint(21) DEFAULT NULL,\n"
                + "\tTIME_RESET datetime DEFAULT NULL,\n"
                + "\tSTATUS varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tTYPE varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tCOMMENT varchar(193) DEFAULT '' NOT NULL\n"
                + ")";

        public String NAME;

        public String SUBSYSTEM;

        public Long COUNT;

        public Long MAX_COUNT;

        public Long MIN_COUNT;

        public Double AVG_COUNT;

        public Long COUNT_RESET;

        public Long MAX_COUNT_RESET;

        public Long MIN_COUNT_RESET;

        public Double AVG_COUNT_RESET;

        public Long TIME_ENABLED;

        public Long TIME_DISABLED;

        public Long TIME_ELAPSED;

        public Long TIME_RESET;

        public String STATUS;

        public String TYPE;

        public String COMMENT;
    }

    public static class SPATIAL_REF_SYS {
        public static final String createTableSQL = "CREATE TABLE `SPATIAL_REF_SYS` (\n"
                + "\tSRID smallint(5) DEFAULT 0 NOT NULL,\n"
                + "\tAUTH_NAME varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tAUTH_SRID int(5) DEFAULT 0 NOT NULL,\n"
                + "\tSRTEXT varchar(2048) DEFAULT '' NOT NULL\n"
                + ")";

        public Short SRID;

        public String AUTH_NAME;

        public Integer AUTH_SRID;

        public String SRTEXT;
    }

    public static class INNODB_CMP_RESET {
        public static final String createTableSQL = "CREATE TABLE `INNODB_CMP_RESET` (\n"
                + "\tpage_size int(5) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_ops_ok int(11) DEFAULT 0 NOT NULL,\n"
                + "\tcompress_time int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_ops int(11) DEFAULT 0 NOT NULL,\n"
                + "\tuncompress_time int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Integer page_size;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    public static class INNODB_FT_INDEX_CACHE {
        public static final String createTableSQL = "CREATE TABLE `INNODB_FT_INDEX_CACHE` (\n"
                + "\tWORD varchar(337) DEFAULT '' NOT NULL,\n"
                + "\tFIRST_DOC_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tLAST_DOC_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDOC_COUNT bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tDOC_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tPOSITION bigint(21) DEFAULT 0 NOT NULL\n"
                + ")";

        public String WORD;

        public Long FIRST_DOC_ID;

        public Long LAST_DOC_ID;

        public Long DOC_COUNT;

        public Long DOC_ID;

        public Long POSITION;
    }

    public static class INNODB_SYS_INDEXES {
        public static final String createTableSQL = "CREATE TABLE `INNODB_SYS_INDEXES` (\n"
                + "\tINDEX_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tNAME varchar(193) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_ID bigint(21) DEFAULT 0 NOT NULL,\n"
                + "\tTYPE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tN_FIELDS int(11) DEFAULT 0 NOT NULL,\n"
                + "\tPAGE_NO int(11) DEFAULT 0 NOT NULL,\n"
                + "\tSPACE int(11) DEFAULT 0 NOT NULL,\n"
                + "\tMERGE_THRESHOLD int(11) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long INDEX_ID;

        public String NAME;

        public Long TABLE_ID;

        public Integer TYPE;

        public Integer N_FIELDS;

        public Integer PAGE_NO;

        public Integer SPACE;

        public Integer MERGE_THRESHOLD;
    }

    public static class USER_PRIVILEGES {
        public static final String createTableSQL = "CREATE TABLE `USER_PRIVILEGES` (\n"
                + "\tGRANTEE varchar(190) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tPRIVILEGE_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tIS_GRANTABLE varchar(3) DEFAULT '' NOT NULL\n"
                + ")";

        public String GRANTEE;

        public String TABLE_CATALOG;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    public static class PROCESSLIST {
        public static final String createTableSQL = "CREATE TABLE `PROCESSLIST` (\n"
                + "\tID bigint(4) DEFAULT 0 NOT NULL,\n"
                + "\tUSER varchar(128) DEFAULT '' NOT NULL,\n"
                + "\tHOST varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tDB varchar(64) DEFAULT NULL,\n"
                + "\tCOMMAND varchar(16) DEFAULT '' NOT NULL,\n"
                + "\tTIME int(7) DEFAULT 0 NOT NULL,\n"
                + "\tSTATE varchar(64) DEFAULT NULL,\n"
                + "\tINFO longtext DEFAULT NULL,\n"
                + "\tTIME_MS decimal(22, 3) DEFAULT 0.000 NOT NULL,\n"
                + "\tSTAGE tinyint(2) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_STAGE tinyint(2) DEFAULT 0 NOT NULL,\n"
                + "\tPROGRESS decimal(7, 3) DEFAULT 0.000 NOT NULL,\n"
                + "\tMEMORY_USED bigint(7) DEFAULT 0 NOT NULL,\n"
                + "\tMAX_MEMORY_USED bigint(7) DEFAULT 0 NOT NULL,\n"
                + "\tEXAMINED_ROWS int(7) DEFAULT 0 NOT NULL,\n"
                + "\tQUERY_ID bigint(4) DEFAULT 0 NOT NULL,\n"
                + "\tINFO_BINARY blob DEFAULT NULL,\n"
                + "\tTID bigint(4) DEFAULT 0 NOT NULL\n"
                + ")";

        public Long ID;

        public String USER;

        public String HOST;

        public String DB;

        public String COMMAND;

        public Integer TIME;

        public String STATE;

        public String INFO;

        public BigDecimal TIME_MS;

        public Short STAGE;

        public Short MAX_STAGE;

        public BigDecimal PROGRESS;

        public Long MEMORY_USED;

        public Long MAX_MEMORY_USED;

        public Integer EXAMINED_ROWS;

        public Long QUERY_ID;

        public byte[] INFO_BINARY;

        public Long TID;
    }

    public static class SCHEMA_PRIVILEGES {
        public static final String createTableSQL = "CREATE TABLE `SCHEMA_PRIVILEGES` (\n"
                + "\tGRANTEE varchar(190) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tTABLE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tPRIVILEGE_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tIS_GRANTABLE varchar(3) DEFAULT '' NOT NULL\n"
                + ")";

        public String GRANTEE;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    public static class ROUTINES {
        public static final String createTableSQL = "CREATE TABLE `ROUTINES` (\n"
                + "\tSPECIFIC_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tROUTINE_CATALOG varchar(512) DEFAULT '' NOT NULL,\n"
                + "\tROUTINE_SCHEMA varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tROUTINE_NAME varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tROUTINE_TYPE varchar(13) DEFAULT '' NOT NULL,\n"
                + "\tDATA_TYPE varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_MAXIMUM_LENGTH int(21) DEFAULT NULL,\n"
                + "\tCHARACTER_OCTET_LENGTH int(21) DEFAULT NULL,\n"
                + "\tNUMERIC_PRECISION int(21) DEFAULT NULL,\n"
                + "\tNUMERIC_SCALE int(21) DEFAULT NULL,\n"
                + "\tDATETIME_PRECISION bigint(21) DEFAULT NULL,\n"
                + "\tCHARACTER_SET_NAME varchar(64) DEFAULT NULL,\n"
                + "\tCOLLATION_NAME varchar(64) DEFAULT NULL,\n"
                + "\tDTD_IDENTIFIER longtext DEFAULT NULL,\n"
                + "\tROUTINE_BODY varchar(8) DEFAULT '' NOT NULL,\n"
                + "\tROUTINE_DEFINITION longtext DEFAULT NULL,\n"
                + "\tEXTERNAL_NAME varchar(64) DEFAULT NULL,\n"
                + "\tEXTERNAL_LANGUAGE varchar(64) DEFAULT NULL,\n"
                + "\tPARAMETER_STYLE varchar(8) DEFAULT '' NOT NULL,\n"
                + "\tIS_DETERMINISTIC varchar(3) DEFAULT '' NOT NULL,\n"
                + "\tSQL_DATA_ACCESS varchar(64) DEFAULT '' NOT NULL,\n"
                + "\tSQL_PATH varchar(64) DEFAULT NULL,\n"
                + "\tSECURITY_TYPE varchar(7) DEFAULT '' NOT NULL,\n"
                + "\tCREATED datetime DEFAULT '0000-00-00 00:00:00' NOT NULL,\n"
                + "\tLAST_ALTERED datetime DEFAULT '0000-00-00 00:00:00' NOT NULL,\n"
                + "\tSQL_MODE varchar(8192) DEFAULT '' NOT NULL,\n"
                + "\tROUTINE_COMMENT longtext DEFAULT '' NOT NULL,\n"
                + "\tDEFINER varchar(189) DEFAULT '' NOT NULL,\n"
                + "\tCHARACTER_SET_CLIENT varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tCOLLATION_CONNECTION varchar(32) DEFAULT '' NOT NULL,\n"
                + "\tDATABASE_COLLATION varchar(32) DEFAULT '' NOT NULL\n"
                + ")";

        public String SPECIFIC_NAME;

        public String ROUTINE_CATALOG;

        public String ROUTINE_SCHEMA;

        public String ROUTINE_NAME;

        public String ROUTINE_TYPE;

        public String DATA_TYPE;

        public Integer CHARACTER_MAXIMUM_LENGTH;

        public Integer CHARACTER_OCTET_LENGTH;

        public Integer NUMERIC_PRECISION;

        public Integer NUMERIC_SCALE;

        public Long DATETIME_PRECISION;

        public String CHARACTER_SET_NAME;

        public String COLLATION_NAME;

        public String DTD_IDENTIFIER;

        public String ROUTINE_BODY;

        public String ROUTINE_DEFINITION;

        public String EXTERNAL_NAME;

        public String EXTERNAL_LANGUAGE;

        public String PARAMETER_STYLE;

        public String IS_DETERMINISTIC;

        public String SQL_DATA_ACCESS;

        public String SQL_PATH;

        public String SECURITY_TYPE;

        public Long CREATED;

        public Long LAST_ALTERED;

        public String SQL_MODE;

        public String ROUTINE_COMMENT;

        public String DEFINER;

        public String CHARACTER_SET_CLIENT;

        public String COLLATION_CONNECTION;

        public String DATABASE_COLLATION;
    }
}
