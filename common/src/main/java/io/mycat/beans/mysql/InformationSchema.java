package io.mycat.beans.mysql;


import lombok.*;

import java.math.BigDecimal;

/**
 * @author Junwen Chen
 */
@NoArgsConstructor
@Data
public class InformationSchema {

    public TABLES_TABLE_OBJECT[] TABLES = new TABLES_TABLE_OBJECT[]{};

    public INNODB_SYS_COLUMNS_TABLE_OBJECT[] INNODB_SYS_COLUMNS = new INNODB_SYS_COLUMNS_TABLE_OBJECT[]{};

    public INNODB_SYS_TABLESPACES_TABLE_OBJECT[] INNODB_SYS_TABLESPACES = new INNODB_SYS_TABLESPACES_TABLE_OBJECT[]{};

    public INNODB_FT_INDEX_TABLE_TABLE_OBJECT[] INNODB_FT_INDEX_TABLE = new INNODB_FT_INDEX_TABLE_TABLE_OBJECT[]{};

    public CHECK_CONSTRAINTS_TABLE_OBJECT[] CHECK_CONSTRAINTS = new CHECK_CONSTRAINTS_TABLE_OBJECT[]{};

    public PARTITIONS_TABLE_OBJECT[] PARTITIONS = new PARTITIONS_TABLE_OBJECT[]{};

    public TABLE_PRIVILEGES_TABLE_OBJECT[] TABLE_PRIVILEGES = new TABLE_PRIVILEGES_TABLE_OBJECT[]{};

    public TRIGGERS_TABLE_OBJECT[] TRIGGERS = new TRIGGERS_TABLE_OBJECT[]{};

    public GEOMETRY_COLUMNS_TABLE_OBJECT[] GEOMETRY_COLUMNS = new GEOMETRY_COLUMNS_TABLE_OBJECT[]{};

    public EVENTS_TABLE_OBJECT[] EVENTS = new EVENTS_TABLE_OBJECT[]{};

    public PARAMETERS_TABLE_OBJECT[] PARAMETERS = new PARAMETERS_TABLE_OBJECT[]{};

    public INNODB_FT_DEFAULT_STOPWORD_TABLE_OBJECT[] INNODB_FT_DEFAULT_STOPWORD = new INNODB_FT_DEFAULT_STOPWORD_TABLE_OBJECT[]{};

    public INNODB_TABLESPACES_SCRUBBING_TABLE_OBJECT[] INNODB_TABLESPACES_SCRUBBING = new INNODB_TABLESPACES_SCRUBBING_TABLE_OBJECT[]{};

    public INNODB_LOCK_WAITS_TABLE_OBJECT[] INNODB_LOCK_WAITS = new INNODB_LOCK_WAITS_TABLE_OBJECT[]{};

    public FILES_TABLE_OBJECT[] FILES = new FILES_TABLE_OBJECT[]{};

    public PLUGINS_TABLE_OBJECT[] PLUGINS = new PLUGINS_TABLE_OBJECT[]{};

    public GLOBAL_STATUS_TABLE_OBJECT[] GLOBAL_STATUS = new GLOBAL_STATUS_TABLE_OBJECT[]{};

    public ALL_PLUGINS_TABLE_OBJECT[] ALL_PLUGINS = new ALL_PLUGINS_TABLE_OBJECT[]{};

    public USER_STATISTICS_TABLE_OBJECT[] USER_STATISTICS = new USER_STATISTICS_TABLE_OBJECT[]{};

    public INNODB_SYS_TABLESTATS_TABLE_OBJECT[] INNODB_SYS_TABLESTATS = new INNODB_SYS_TABLESTATS_TABLE_OBJECT[]{};

    public INNODB_SYS_SEMAPHORE_WAITS_TABLE_OBJECT[] INNODB_SYS_SEMAPHORE_WAITS = new INNODB_SYS_SEMAPHORE_WAITS_TABLE_OBJECT[]{};

    public INNODB_CMP_PER_INDEX_RESET_TABLE_OBJECT[] INNODB_CMP_PER_INDEX_RESET = new INNODB_CMP_PER_INDEX_RESET_TABLE_OBJECT[]{};

    public TABLE_STATISTICS_TABLE_OBJECT[] TABLE_STATISTICS = new TABLE_STATISTICS_TABLE_OBJECT[]{};

    public COLLATION_CHARACTER_SET_APPLICABILITY_TABLE_OBJECT[] COLLATION_CHARACTER_SET_APPLICABILITY = new COLLATION_CHARACTER_SET_APPLICABILITY_TABLE_OBJECT[]{};

    public ENGINES_TABLE_OBJECT[] ENGINES = new ENGINES_TABLE_OBJECT[]{};

    public KEY_COLUMN_USAGE_TABLE_OBJECT[] KEY_COLUMN_USAGE = new KEY_COLUMN_USAGE_TABLE_OBJECT[]{};

    public GLOBAL_VARIABLES_TABLE_OBJECT[] GLOBAL_VARIABLES = new GLOBAL_VARIABLES_TABLE_OBJECT[]{};

    public INNODB_LOCKS_TABLE_OBJECT[] INNODB_LOCKS = new INNODB_LOCKS_TABLE_OBJECT[]{};

    public INNODB_SYS_FOREIGN_COLS_TABLE_OBJECT[] INNODB_SYS_FOREIGN_COLS = new INNODB_SYS_FOREIGN_COLS_TABLE_OBJECT[]{};

    public INNODB_BUFFER_PAGE_TABLE_OBJECT[] INNODB_BUFFER_PAGE = new INNODB_BUFFER_PAGE_TABLE_OBJECT[]{};

    public SESSION_STATUS_TABLE_OBJECT[] SESSION_STATUS = new SESSION_STATUS_TABLE_OBJECT[]{};

    public INNODB_SYS_DATAFILES_TABLE_OBJECT[] INNODB_SYS_DATAFILES = new INNODB_SYS_DATAFILES_TABLE_OBJECT[]{};

    public INNODB_CMP_TABLE_OBJECT[] INNODB_CMP = new INNODB_CMP_TABLE_OBJECT[]{};

    public KEY_CACHES_TABLE_OBJECT[] KEY_CACHES = new KEY_CACHES_TABLE_OBJECT[]{};

    public INNODB_CMPMEM_RESET_TABLE_OBJECT[] INNODB_CMPMEM_RESET = new INNODB_CMPMEM_RESET_TABLE_OBJECT[]{};

    public INNODB_SYS_VIRTUAL_TABLE_OBJECT[] INNODB_SYS_VIRTUAL = new INNODB_SYS_VIRTUAL_TABLE_OBJECT[]{};

    public APPLICABLE_ROLES_TABLE_OBJECT[] APPLICABLE_ROLES = new APPLICABLE_ROLES_TABLE_OBJECT[]{};

    public INNODB_SYS_FIELDS_TABLE_OBJECT[] INNODB_SYS_FIELDS = new INNODB_SYS_FIELDS_TABLE_OBJECT[]{};

    public TABLESPACES_TABLE_OBJECT[] TABLESPACES = new TABLESPACES_TABLE_OBJECT[]{};

    public REFERENTIAL_CONSTRAINTS_TABLE_OBJECT[] REFERENTIAL_CONSTRAINTS = new REFERENTIAL_CONSTRAINTS_TABLE_OBJECT[]{};

    public INNODB_SYS_TABLES_TABLE_OBJECT[] INNODB_SYS_TABLES = new INNODB_SYS_TABLES_TABLE_OBJECT[]{};

    public SCHEMATA_TABLE_OBJECT[] SCHEMATA = new SCHEMATA_TABLE_OBJECT[]{};

    public INNODB_FT_BEING_DELETED_TABLE_OBJECT[] INNODB_FT_BEING_DELETED = new INNODB_FT_BEING_DELETED_TABLE_OBJECT[]{};

    public ENABLED_ROLES_TABLE_OBJECT[] ENABLED_ROLES = new ENABLED_ROLES_TABLE_OBJECT[]{};

    public COLUMNS_TABLE_OBJECT[] COLUMNS = new COLUMNS_TABLE_OBJECT[]{};

    public INNODB_BUFFER_POOL_STATS_TABLE_OBJECT[] INNODB_BUFFER_POOL_STATS = new INNODB_BUFFER_POOL_STATS_TABLE_OBJECT[]{};

    public INNODB_CMP_PER_INDEX_TABLE_OBJECT[] INNODB_CMP_PER_INDEX = new INNODB_CMP_PER_INDEX_TABLE_OBJECT[]{};

    public INNODB_MUTEXES_TABLE_OBJECT[] INNODB_MUTEXES = new INNODB_MUTEXES_TABLE_OBJECT[]{};

    public INNODB_BUFFER_PAGE_LRU_TABLE_OBJECT[] INNODB_BUFFER_PAGE_LRU = new INNODB_BUFFER_PAGE_LRU_TABLE_OBJECT[]{};

    public INNODB_FT_CONFIG_TABLE_OBJECT[] INNODB_FT_CONFIG = new INNODB_FT_CONFIG_TABLE_OBJECT[]{};

    public SYSTEM_VARIABLES_TABLE_OBJECT[] SYSTEM_VARIABLES = new SYSTEM_VARIABLES_TABLE_OBJECT[]{};

    public TABLE_CONSTRAINTS_TABLE_OBJECT[] TABLE_CONSTRAINTS = new TABLE_CONSTRAINTS_TABLE_OBJECT[]{};

    public CLIENT_STATISTICS_TABLE_OBJECT[] CLIENT_STATISTICS = new CLIENT_STATISTICS_TABLE_OBJECT[]{};

    public PROFILING_TABLE_OBJECT[] PROFILING = new PROFILING_TABLE_OBJECT[]{};

    public INNODB_TABLESPACES_ENCRYPTION_TABLE_OBJECT[] INNODB_TABLESPACES_ENCRYPTION = new INNODB_TABLESPACES_ENCRYPTION_TABLE_OBJECT[]{};

    public INNODB_SYS_FOREIGN_TABLE_OBJECT[] INNODB_SYS_FOREIGN = new INNODB_SYS_FOREIGN_TABLE_OBJECT[]{};

    public COLLATIONS_TABLE_OBJECT[] COLLATIONS = new COLLATIONS_TABLE_OBJECT[]{};

    public INNODB_CMPMEM_TABLE_OBJECT[] INNODB_CMPMEM = new INNODB_CMPMEM_TABLE_OBJECT[]{};

    public INNODB_TRX_TABLE_OBJECT[] INNODB_TRX = new INNODB_TRX_TABLE_OBJECT[]{};

    public CHARACTER_SETS_TABLE_OBJECT[] CHARACTER_SETS = new CHARACTER_SETS_TABLE_OBJECT[]{};

    public INDEX_STATISTICS_TABLE_OBJECT[] INDEX_STATISTICS = new INDEX_STATISTICS_TABLE_OBJECT[]{};

    public INNODB_FT_DELETED_TABLE_OBJECT[] INNODB_FT_DELETED = new INNODB_FT_DELETED_TABLE_OBJECT[]{};

    public STATISTICS_TABLE_OBJECT[] STATISTICS = new STATISTICS_TABLE_OBJECT[]{};

    public VIEWS_TABLE_OBJECT[] VIEWS = new VIEWS_TABLE_OBJECT[]{};

    public COLUMN_PRIVILEGES_TABLE_OBJECT[] COLUMN_PRIVILEGES = new COLUMN_PRIVILEGES_TABLE_OBJECT[]{};

    public user_variables_TABLE_OBJECT[] user_variables = new user_variables_TABLE_OBJECT[]{};

    public SESSION_VARIABLES_TABLE_OBJECT[] SESSION_VARIABLES = new SESSION_VARIABLES_TABLE_OBJECT[]{};

    public INNODB_METRICS_TABLE_OBJECT[] INNODB_METRICS = new INNODB_METRICS_TABLE_OBJECT[]{};

    public SPATIAL_REF_SYS_TABLE_OBJECT[] SPATIAL_REF_SYS = new SPATIAL_REF_SYS_TABLE_OBJECT[]{};

    public INNODB_CMP_RESET_TABLE_OBJECT[] INNODB_CMP_RESET = new INNODB_CMP_RESET_TABLE_OBJECT[]{};

    public INNODB_FT_INDEX_CACHE_TABLE_OBJECT[] INNODB_FT_INDEX_CACHE = new INNODB_FT_INDEX_CACHE_TABLE_OBJECT[]{};

    public INNODB_SYS_INDEXES_TABLE_OBJECT[] INNODB_SYS_INDEXES = new INNODB_SYS_INDEXES_TABLE_OBJECT[]{};

    public USER_PRIVILEGES_TABLE_OBJECT[] USER_PRIVILEGES = new USER_PRIVILEGES_TABLE_OBJECT[]{};

    public PROCESSLIST_TABLE_OBJECT[] PROCESSLIST = new PROCESSLIST_TABLE_OBJECT[]{};

    public SCHEMA_PRIVILEGES_TABLE_OBJECT[] SCHEMA_PRIVILEGES = new SCHEMA_PRIVILEGES_TABLE_OBJECT[]{};

    public ROUTINES_TABLE_OBJECT[] ROUTINES = new ROUTINES_TABLE_OBJECT[]{};

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TABLES_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_COLUMNS_TABLE_OBJECT {
        public Long TABLE_ID;

        public String NAME;

        public Long POS;

        public Integer MTYPE;

        public Integer PRTYPE;

        public Integer LEN;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_TABLESPACES_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_FT_INDEX_TABLE_TABLE_OBJECT {
        public String WORD;

        public Long FIRST_DOC_ID;

        public Long LAST_DOC_ID;

        public Long DOC_COUNT;

        public Long DOC_ID;

        public Long POSITION;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class CHECK_CONSTRAINTS_TABLE_OBJECT {
        public String CONSTRAINT_CATALOG;

        public String CONSTRAINT_SCHEMA;

        public String CONSTRAINT_NAME;

        public String TABLE_NAME;

        public String CHECK_CLAUSE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class PARTITIONS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TABLE_PRIVILEGES_TABLE_OBJECT {
        public String GRANTEE;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TRIGGERS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class GEOMETRY_COLUMNS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class EVENTS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class PARAMETERS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_FT_DEFAULT_STOPWORD_TABLE_OBJECT {
        public String value;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_TABLESPACES_SCRUBBING_TABLE_OBJECT {
        public Long SPACE;

        public String NAME;

        public Integer COMPRESSED;

        public Long LAST_SCRUB_COMPLETED;

        public Long CURRENT_SCRUB_STARTED;

        public Integer CURRENT_SCRUB_ACTIVE_THREADS;

        public Long CURRENT_SCRUB_PAGE_NUMBER;

        public Long CURRENT_SCRUB_MAX_PAGE_NUMBER;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_LOCK_WAITS_TABLE_OBJECT {
        public String requesting_trx_id;

        public String requested_lock_id;

        public String blocking_trx_id;

        public String blocking_lock_id;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class FILES_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class PLUGINS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class GLOBAL_STATUS_TABLE_OBJECT {
        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class ALL_PLUGINS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class USER_STATISTICS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_TABLESTATS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_SEMAPHORE_WAITS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_CMP_PER_INDEX_RESET_TABLE_OBJECT {
        public String database_name;

        public String table_name;

        public String index_name;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TABLE_STATISTICS_TABLE_OBJECT {
        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public Long ROWS_READ;

        public Long ROWS_CHANGED;

        public Long ROWS_CHANGED_X_INDEXES;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class COLLATION_CHARACTER_SET_APPLICABILITY_TABLE_OBJECT {
        public String COLLATION_NAME;

        public String CHARACTER_SET_NAME;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class ENGINES_TABLE_OBJECT {
        public String ENGINE;

        public String SUPPORT;

        public String COMMENT;

        public String TRANSACTIONS;

        public String XA;

        public String SAVEPOINTS;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class KEY_COLUMN_USAGE_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class GLOBAL_VARIABLES_TABLE_OBJECT {
        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_LOCKS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_FOREIGN_COLS_TABLE_OBJECT {
        public String ID;

        public String FOR_COL_NAME;

        public String REF_COL_NAME;

        public Integer POS;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_BUFFER_PAGE_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class SESSION_STATUS_TABLE_OBJECT {
        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_DATAFILES_TABLE_OBJECT {
        public Integer SPACE;

        public String PATH;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_CMP_TABLE_OBJECT {
        public Integer page_size;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class KEY_CACHES_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_CMPMEM_RESET_TABLE_OBJECT {
        public Integer page_size;

        public Integer buffer_pool_instance;

        public Integer pages_used;

        public Integer pages_free;

        public Long relocation_ops;

        public Integer relocation_time;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_VIRTUAL_TABLE_OBJECT {
        public Long TABLE_ID;

        public Integer POS;

        public Integer BASE_POS;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class APPLICABLE_ROLES_TABLE_OBJECT {
        public String GRANTEE;

        public String ROLE_NAME;

        public String IS_GRANTABLE;

        public String IS_DEFAULT;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_FIELDS_TABLE_OBJECT {
        public Long INDEX_ID;

        public String NAME;

        public Integer POS;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TABLESPACES_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class REFERENTIAL_CONSTRAINTS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_TABLES_TABLE_OBJECT {
        public Long TABLE_ID;

        public String NAME;

        public Integer FLAG;

        public Integer N_COLS;

        public Integer SPACE;

        public String ROW_FORMAT;

        public Integer ZIP_PAGE_SIZE;

        public String SPACE_TYPE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class SCHEMATA_TABLE_OBJECT {
        public String CATALOG_NAME;

        public String SCHEMA_NAME;

        public String DEFAULT_CHARACTER_SET_NAME;

        public String DEFAULT_COLLATION_NAME;

        public String SQL_PATH;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_FT_BEING_DELETED_TABLE_OBJECT {
        public Long DOC_ID;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class ENABLED_ROLES_TABLE_OBJECT {
        public String ROLE_NAME;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class COLUMNS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_BUFFER_POOL_STATS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_CMP_PER_INDEX_TABLE_OBJECT {
        public String database_name;

        public String table_name;

        public String index_name;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_MUTEXES_TABLE_OBJECT {
        public String NAME;

        public String CREATE_FILE;

        public Integer CREATE_LINE;

        public Long OS_WAITS;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_BUFFER_PAGE_LRU_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_FT_CONFIG_TABLE_OBJECT {
        public String KEY;

        public String VALUE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class SYSTEM_VARIABLES_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TABLE_CONSTRAINTS_TABLE_OBJECT {
        public String CONSTRAINT_CATALOG;

        public String CONSTRAINT_SCHEMA;

        public String CONSTRAINT_NAME;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String CONSTRAINT_TYPE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class CLIENT_STATISTICS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class PROFILING_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_TABLESPACES_ENCRYPTION_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_FOREIGN_TABLE_OBJECT {
        public String ID;

        public String FOR_NAME;

        public String REF_NAME;

        public Integer N_COLS;

        public Integer TYPE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class COLLATIONS_TABLE_OBJECT {
        public String COLLATION_NAME;

        public String CHARACTER_SET_NAME;

        public Long ID;

        public String IS_DEFAULT;

        public String IS_COMPILED;

        public Long SORTLEN;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_CMPMEM_TABLE_OBJECT {
        public Integer page_size;

        public Integer buffer_pool_instance;

        public Integer pages_used;

        public Integer pages_free;

        public Long relocation_ops;

        public Integer relocation_time;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_TRX_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class CHARACTER_SETS_TABLE_OBJECT {
        public String CHARACTER_SET_NAME;

        public String DEFAULT_COLLATE_NAME;

        public String DESCRIPTION;

        public Long MAXLEN;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INDEX_STATISTICS_TABLE_OBJECT {
        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String INDEX_NAME;

        public Long ROWS_READ;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_FT_DELETED_TABLE_OBJECT {
        public Long DOC_ID;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class STATISTICS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class VIEWS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class COLUMN_PRIVILEGES_TABLE_OBJECT {
        public String GRANTEE;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String TABLE_NAME;

        public String COLUMN_NAME;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class user_variables_TABLE_OBJECT {
        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;

        public String VARIABLE_TYPE;

        public String CHARACTER_SET_NAME;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class SESSION_VARIABLES_TABLE_OBJECT {
        public String VARIABLE_NAME;

        public String VARIABLE_VALUE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_METRICS_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class SPATIAL_REF_SYS_TABLE_OBJECT {
        public Short SRID;

        public String AUTH_NAME;

        public Integer AUTH_SRID;

        public String SRTEXT;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_CMP_RESET_TABLE_OBJECT {
        public Integer page_size;

        public Integer compress_ops;

        public Integer compress_ops_ok;

        public Integer compress_time;

        public Integer uncompress_ops;

        public Integer uncompress_time;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_FT_INDEX_CACHE_TABLE_OBJECT {
        public String WORD;

        public Long FIRST_DOC_ID;

        public Long LAST_DOC_ID;

        public Long DOC_COUNT;

        public Long DOC_ID;

        public Long POSITION;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class INNODB_SYS_INDEXES_TABLE_OBJECT {
        public Long INDEX_ID;

        public String NAME;

        public Long TABLE_ID;

        public Integer TYPE;

        public Integer N_FIELDS;

        public Integer PAGE_NO;

        public Integer SPACE;

        public Integer MERGE_THRESHOLD;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class USER_PRIVILEGES_TABLE_OBJECT {
        public String GRANTEE;

        public String TABLE_CATALOG;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class PROCESSLIST_TABLE_OBJECT {
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

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class SCHEMA_PRIVILEGES_TABLE_OBJECT {
        public String GRANTEE;

        public String TABLE_CATALOG;

        public String TABLE_SCHEMA;

        public String PRIVILEGE_TYPE;

        public String IS_GRANTABLE;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class ROUTINES_TABLE_OBJECT {
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