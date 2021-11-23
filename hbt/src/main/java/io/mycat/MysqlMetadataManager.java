package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.SchemaHandlerImpl;
import io.mycat.config.LogicSchemaConfig;
import io.mycat.prototypeserver.mysql.*;
import io.mycat.util.NameMap;
import io.reactivex.rxjava3.core.Observable;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MysqlMetadataManager extends MetadataManager {
    public static final NameMap<String> INFORMATION_SCHEMA_INFO = NameMap.immutableCopyOf(CreateMySQLSQLSet.getFieldValue(InformationSchema.class));
    public static final NameMap<String> PERFORMANCE_SCHEMA_INFO = NameMap.immutableCopyOf(CreateMySQLSQLSet.getFieldValue(PerformanceSchema.class));
    public static final NameMap<String> MYSQL_SCHEMA_INFO = NameMap.immutableCopyOf(CreateMySQLSQLSet.getFieldValue(MysqlSchema.class));

    public static final VisualTableHandler CHARACTER_SETS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createCHARACTER_SETSTableSQL(),
            () -> Observable.fromArray(
                    new Object[]{"utf8", "utf8_bin", "UTF-8 Unicode", 3},
                    new Object[]{"utf8mb4", "utf8mb4_bin", "UTF-8 Unicode", 4},
                    new Object[]{"ascii", "ascii_bin", "US ASCII", 1},
                    new Object[]{"latin1", "latin1_bin", "Latin1", 1},
                    new Object[]{"binary", "binary", "binary", 1}
            ));
    public static final VisualTableHandler COLLATIONS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createCOLLATIONSTableSQL(), () -> {
        List<Object[]> list = Collections.singletonList(new Object[]{"utf8mb4_bin", "utf8mb4", 46, "Yes", "Yes", 1,"PAD SPACE"});
        return Observable.fromIterable(list);
    });

    public static final VisualTableHandler COLLATION_CHARACTER_SET_APPLICABILITY_TABLE_HANDLER =
            VisualTableHandler.createByMySQL(createCOLLATION_CHARACTER_SET_APPLICABILITYTableSQL(), () -> {
                List<Object[]> list = Collections.singletonList(new Object[]{"utf8mb4_bin", "utf8mb4"});
                return Observable.fromIterable(list);
            });
    public static final VisualTableHandler COLUMNS_TABLE_HANDLER =
            VisualTableHandler.createByMySQL(createColumnsTableSQL(), new Supplier<Observable<Object[]>>() {
                @Override
                public Observable<Object[]> get() {
                    ArrayList<Object[]> objects = new ArrayList<>();

                    MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                    List<TableHandler> tableHandlers = metadataManager.getSchemaMap().values().stream().flatMap(i -> i.logicTables().values().stream()).collect(Collectors.toList());
                    for (TableHandler tableHandler : tableHandlers) {
                        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(tableHandler.getCreateTableSQL());
                        if (!(sqlStatement instanceof MySqlCreateTableStatement)) {
                            continue;
                        }
                        MySqlCreateTableStatement mySqlCreateTableStatement = (MySqlCreateTableStatement) sqlStatement;

                        List<SQLColumnDefinition> columns = new ArrayList<SQLColumnDefinition>();
                        List<String> dataTypes = new ArrayList<String>();
                        List<String> defaultValues = new ArrayList<String>();

                        int name_len = -1, dataType_len = -1, defaultVal_len = 7, extra_len = 5;
                        for (SQLTableElement element : mySqlCreateTableStatement.getTableElementList()) {
                            if (element instanceof SQLColumnDefinition) {
                                SQLColumnDefinition column = (SQLColumnDefinition) element;
                                columns.add(column);

                                String name = SQLUtils.normalize(column.getName().getSimpleName());
                                if (name_len < name.length()) {
                                    name_len = name.length();
                                }

                                String dataType = column.getDataType().getName();
                                if (column.getDataType().getArguments().size() > 0) {
                                    dataType += "(";
                                    for (int i = 0; i < column.getDataType().getArguments().size(); i++) {
                                        if (i != 0) {
                                            dataType += ",";
                                        }
                                        SQLExpr arg = column.getDataType().getArguments().get(i);
                                        dataType += arg.toString();
                                    }
                                    dataType += ")";
                                }

                                if (dataType_len < dataType.length()) {
                                    dataType_len = dataType.length();
                                }
                                dataTypes.add(dataType);

                                if (column.getDefaultExpr() == null) {
                                    defaultValues.add(null);
                                } else {
                                    String defaultVal = SQLUtils.toMySqlString(column.getDefaultExpr());
                                    if (defaultVal.length() > 2 && defaultVal.charAt(0) == '\'' && defaultVal.charAt(defaultVal.length() - 1) == '\'') {
                                        defaultVal = defaultVal.substring(1, defaultVal.length() - 1);
                                    }
                                    defaultValues.add(defaultVal);

                                    if (defaultVal_len < defaultVal.length()) {
                                        defaultVal_len = defaultVal.length();
                                    }
                                }

                                if (column.isAutoIncrement()) {
                                    extra_len = "auto_increment".length();
                                } else if (column.getOnUpdate() != null) {
                                    extra_len = "on update CURRENT_TIMESTAMP".length();
                                }
                            }
                        }

                        String TABLE_CATALOG = "def";
                        String TABLE_SCHEMA = tableHandler.getSchemaName();
                        String TABLE_NAME = tableHandler.getTableName();
                        String COLUMN_NAME;
                        long ORDINAL_POSITION;
                        String COLUMN_DEFAULT;
                        String IS_NULLABLE;
                        String DATA_TYPE;
                        long CHARACTER_MAXIMUM_LENGTH = 4;
                        long CHARACTER_OCTET_LENGTH = 1;
                        long NUMERIC_PRECISION = 0;
                        long NUMERIC_SCALE = 0;
                        long DATETIME_PRECISION = 0;
                        String CHARACTER_SET_NAME = "utf8mb4";
                        String COLLATION_NAME = "utf8_general_ci";
                        String COLUMN_TYPE;
                        String COLUMN_KEY;
                        String EXTRA;
                        String PRIVILEGES;
                        String COLUMN_COMMENT;
                        String GENERATION_EXPRESSION;

                        for (int i = 0; i < columns.size(); i++) {
                            SQLColumnDefinition column = columns.get(i);
                            String name = SQLUtils.normalize(column.getName().getSimpleName());

                            COLUMN_NAME = name;
                            ORDINAL_POSITION = i;
                            DATA_TYPE = column.getDataType().getName();
                            String Collation = Optional.ofNullable(column.getCollateExpr()).map(s -> SQLUtils.normalize(s.toString())).orElse(null);
                            IS_NULLABLE = column.containsNotNullConstaint() ? "NO" : "YES";
                            COLUMN_KEY = mySqlCreateTableStatement.isPrimaryColumn(name) ?
                                    "PRI" : mySqlCreateTableStatement.isUNI(name) ? "UNI" : mySqlCreateTableStatement.isMUL(name) ? "MUL" : null;

                            COLUMN_DEFAULT = Optional.ofNullable(defaultValues.get(i)).orElse("NULL");
                            CHARACTER_MAXIMUM_LENGTH = 4;
                            COLUMN_TYPE = dataTypes.get(i);
                            EXTRA = "";
                            PRIVILEGES = "select,insert,update,references";
                            COLUMN_COMMENT = Optional.ofNullable(column.getComment()).map(s -> ((SQLCharExpr) s).getText()).orElse(null);
                            GENERATION_EXPRESSION = Optional.ofNullable(column.getGeneratedAlawsAs()).map(m -> m.toString()).orElse(null);

                            objects.add(new Object[]{
                                    TABLE_CATALOG,
                                    TABLE_SCHEMA,
                                    TABLE_NAME,
                                    COLUMN_NAME,
                                    ORDINAL_POSITION,
                                    COLUMN_DEFAULT,
                                    IS_NULLABLE,
                                    DATA_TYPE,
                                    CHARACTER_MAXIMUM_LENGTH,
                                    CHARACTER_OCTET_LENGTH,
                                    NUMERIC_PRECISION,
                                    NUMERIC_SCALE,
                                    DATETIME_PRECISION,
                                    CHARACTER_SET_NAME,
                                    COLLATION_NAME,
                                    COLUMN_TYPE,
                                    COLUMN_KEY,
                                    EXTRA,
                                    PRIVILEGES,
                                    COLUMN_COMMENT,
                                    GENERATION_EXPRESSION
                            });

                        }
                    }
                    return Observable.fromIterable(objects);
                }
            });

    public static final VisualTableHandler EVENTS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createEVENTSTableSQL(), () -> Observable.empty());

    public static final VisualTableHandler COLUMN_STATISTICS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createCOLUMN_STATISTICSTableSQL(), () -> Observable.empty());

    public static final VisualTableHandler FILE_TABLE_HANDLER = VisualTableHandler.createByMySQL(createFileTableSQL(), () -> Observable.empty());
    public static final VisualTableHandler INNODB_DATAFILES_TABLE_HANDLER = VisualTableHandler.createByMySQL(createDATAFILESTableSQL(), () -> Observable.empty());
    public static final VisualTableHandler INNODB_FIELDS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createINNODB_FIELDSTableSQL(), () -> Observable.empty());
    public static final VisualTableHandler ENGINES_TABLE_HANDLER = VisualTableHandler.createByMySQL(INFORMATION_SCHEMA_INFO.get("ENGINES"),
            () -> Observable.fromIterable(Collections.singletonList(
                    new Object[]{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"})));
    public static final VisualTableHandler KEY_COLUMN_USAGE_TABLE_HANDLER = VisualTableHandler.createByMySQL(createKEY_COLUMN_USAGETableSQL(),
            () -> Observable.empty());
    public static final VisualTableHandler PARTITIONS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createPARTITIONSTableSQL(),
            () -> Observable.empty());
    public static final VisualTableHandler PROCESSLIST_TABLE_HANDLER = VisualTableHandler.createByMySQL(INFORMATION_SCHEMA_INFO.get("PROCESSLIST"), () -> {
        long ID;
        String USER;
        String HOST;
        String DB;
        String COMMAND;
        long TIME;
        String STATE;
        String INFO;

        Map<Thread, Process> processMap = new LinkedHashMap<>(Process.getProcessMap());

        ArrayList<Object[]> resList = new ArrayList<>();
        long timestamp = System.currentTimeMillis();
        int currentCount = 0;
        for (Map.Entry<Thread, Process> entry : processMap.entrySet()) {
            Thread holdThread = entry.getKey();
            Process process = entry.getValue();

            ID = (process.getId());
            USER = process.getUser();
            HOST = process.getHost();
            DB = process.getDb();
            COMMAND = process.getCommand().name();
            TIME = timestamp - process.getCreateTimestamp().getTime();
            STATE = process.getState().name();
            INFO = process.getInfo();
            resList.add(new Object[]{ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO});
        }
        return Observable.fromIterable(resList);
    });
    public static final VisualTableHandler SCHEMATA_TABLE_HANDLER = VisualTableHandler.createByMySQL(createSCHEMATATableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {

            String CATALOG_NAME = "def";
            String SCHEMA_NAME;
            String DEFAULT_CHARACTER_SET_NAME = "utf8mb4";
            String DEFAULT_COLLATION_NAME = "utf8_general_ci";
            String SQL_PATH = null;

            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            ArrayList<Object[]> resList = new ArrayList<>();

            List<String> strings = metadataManager.showDatabases();
            for (String string : strings) {
                SCHEMA_NAME = string;
                resList.add(new Object[]{CATALOG_NAME, SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME, SQL_PATH});
            }


            return Observable.fromIterable(resList);
        }
    });
    public static final VisualTableHandler SESSION_VARIABLES_TABLE_HANDLER = VisualTableHandler.createByMySQL(createSessionVariablesTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();

            resList.add(new Object[]{"auto_increment_offset", "1"});
            resList.add(new Object[]{"auto_increment_increment", "1"});
            resList.add(new Object[]{"automatic_sp_privileges", "1"});
            resList.add(new Object[]{"avoid_temporal_upgrade", "0"});
            resList.add(new Object[]{"back_log", "80"});
            resList.add(new Object[]{"basedir", System.getProperty("MYCAT_HOME")});
            resList.add(new Object[]{"big_tables", "0"});
            resList.add(new Object[]{"big_tables", "*"});
            return Observable.fromIterable(resList);
        }
    });
    public static final VisualTableHandler GLOBAL_VARIABLES_TABLE_HANDLER = VisualTableHandler.createByMySQL(createGlobalVariablesTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();
            return Observable.fromIterable(resList);
        }
    });
    private static String createSessionVariablesTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("SESSION_VARIABLES");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("VARIABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("VARIABLE_VALUE ", "varchar(1024)");
        return createEVENTSTableSQL.toString();
    }
    private static String createGlobalVariablesTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("GLOBAL_VARIABLES");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("VARIABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("VARIABLE_VALUE ", "varchar(1024)");
        return createEVENTSTableSQL.toString();
    }

    public static final VisualTableHandler STATISTICS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createStatisticsTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();

            String TABLE_CATALOG = "def";
            String TABLE_SCHEMA;
            String TABLE_NAME;
            String NON_UNIQUE;
            String INDEX_SCHEMA;
            String INDEX_NAME;
            long SEQ_IN_INDEX;
            String COLUMN_NAME ;
            String COLLATION = null;
            long CARDINALITY = 0;
            String SUB_PART = null;
            String PACKED = null;
            String NULLABLE = "";
            String INDEX_TYPE = null;
            String COMMENT = null;
            String INDEX_COMMENT = null;
            String IS_VISIBLE = null;
            String Expression = null;

            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            List<TableHandler> tables = metadataManager.getSchemaMap().values().stream().flatMap(i -> i.logicTables().values().stream()).collect(Collectors.toList());
            for (TableHandler table : tables) {
                TABLE_SCHEMA= table.getSchemaName();
                TABLE_NAME = table.getTableName();
                String createTableSQL = table.getCreateTableSQL();
                SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(createTableSQL);
                if (sqlStatement instanceof MySqlCreateTableStatement){
                    MySqlCreateTableStatement mySqlCreateTableStatement = (MySqlCreateTableStatement) sqlStatement;
                    List<MySqlTableIndex> mySqlTableIndices = Optional.ofNullable(mySqlCreateTableStatement.getMysqlIndexes()).orElse(Collections.emptyList());
                    List<MySqlKey> mySqlKeys = Optional.ofNullable(mySqlCreateTableStatement.getMysqlKeys()).orElse(Collections.emptyList());
                    INDEX_SCHEMA = table.getSchemaName();
                    for (MySqlTableIndex mySqlTableIndex : mySqlTableIndices) {
                        INDEX_NAME = SQLUtils.normalize(mySqlTableIndex.getName().getSimpleName());
                        for (SQLSelectOrderByItem column : mySqlTableIndex.getColumns()) {
                            COLUMN_NAME = SQLUtils.normalize (((SQLName) column.getExpr()).getSimpleName());
                            NON_UNIQUE=  mySqlCreateTableStatement.isUNI(COLUMN_NAME)?"0":"1";
                            SEQ_IN_INDEX =table.getColumnByName(COLUMN_NAME).getId();
                            INDEX_TYPE = mySqlTableIndex.getIndexType();
                            resList.add(new Object[]{
                                    TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE,INDEX_SCHEMA,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME,
                                    COLLATION,CARDINALITY,SUB_PART,PACKED,NULLABLE,INDEX_TYPE,COMMENT,INDEX_COMMENT,IS_VISIBLE,Expression});
                        }

                    }
                }

            }


            return Observable.fromIterable(resList);
        }
    });


    public static void main(String[] args) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement("ALTER TABLE t_order ADD UNIQUE GLOBAL INDEX `g_i_buyer` (`buyer_id`) COVERING (`order_snapshot`) dbpartition by hash(`buyer_id`);");
        System.out.println();
    }
    public static final VisualTableHandler TABLES_CONSTRAINTS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createTableConstraintsTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();

            String CONSTRAINT_CATALOG = "def";
            String CONSTRAINT_SCHEMA;
            String CONSTRAINT_NAME    ;
            String TABLE_SCHEMA       ;
            String TABLE_NAME         ;

            return Observable.fromIterable(resList);
        }
    });
    public static final VisualTableHandler VIEW_TABLE_HANDLER = VisualTableHandler.createByMySQL(createViewTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();


            return Observable.fromIterable(resList);
        }
    });
    public static final VisualTableHandler ROUTINES_TABLE_HANDLER = VisualTableHandler.createByMySQL(createROUTINESTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();


            return Observable.fromIterable(resList);
        }
    });
    public static final VisualTableHandler TRIGGERS_TABLE_HANDLER = VisualTableHandler.createByMySQL(createTRIGGERSTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();


            return Observable.fromIterable(resList);
        }
    });


    private static String createViewTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("views");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("TABLE_CATALOG", "varchar(512)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("VIEW_DEFINITION", "longblob");
        createEVENTSTableSQL.addColumn("CHECK_OPTION", "varchar(8)");
        createEVENTSTableSQL.addColumn("IS_UPDATABLE", "varchar(3)");
        createEVENTSTableSQL.addColumn("DEFINER", "varchar(77)");
        createEVENTSTableSQL.addColumn("SECURITY_TYPE", "varchar(7)");
        createEVENTSTableSQL.addColumn("CHARACTER_SET_CLIENT", "varchar(32)");
        createEVENTSTableSQL.addColumn("COLLATION_CONNECTION", "varchar(32)");
        return createEVENTSTableSQL.toString();
    }

    public static final VisualTableHandler VIEWS_HANDLER = VisualTableHandler.createByMySQL(createTableConstraintsTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();

            String CONSTRAINT_CATALOG = "def";
            String CONSTRAINT_SCHEMA;
            String CONSTRAINT_NAME    ;
            String TABLE_SCHEMA       ;
            String TABLE_NAME         ;

            return Observable.fromIterable(resList);
        }
    });

    private static String createTableConstraintsTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("table_constraints");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("CONSTRAINT_CATALOG", "varchar(512)");
        createEVENTSTableSQL.addColumn("CONSTRAINT_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("CONSTRAINT_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("CONSTRAINT_TYPE", "varchar(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createTablesTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("TABLES");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("TABLES", "varchar(512)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_TYPE", "varchar(64)");
        createEVENTSTableSQL.addColumn("ENGINE", "varchar(64)");
        createEVENTSTableSQL.addColumn("VERSION", "bigint(21)");
        createEVENTSTableSQL.addColumn("ROW_FORMAT", "bigint(18)");
        createEVENTSTableSQL.addColumn("TABLE_ROWS", "bigint(21)");
        createEVENTSTableSQL.addColumn("AVG_ROW_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("DATA_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("MAX_DATA_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("INDEX_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("DATA_FREE", "bigint(21)");
        createEVENTSTableSQL.addColumn("AUTO_INCREMENT", "bigint(21)");
        createEVENTSTableSQL.addColumn("CREATE_TIME", "datetime");
        createEVENTSTableSQL.addColumn("UPDATE_TIME", "datetime");
        createEVENTSTableSQL.addColumn("CHECK_TIME", "datetime");
        createEVENTSTableSQL.addColumn("TABLE_COLLATION", "varchar(32)");
        createEVENTSTableSQL.addColumn("CHECKSUM", "bigint(21)");
        createEVENTSTableSQL.addColumn("CREATE_OPTIONS", "varchar(255)");
        createEVENTSTableSQL.addColumn("TABLE_COMMENT", "varchar(2048)");
        return createEVENTSTableSQL.toString();
    }
    public static final VisualTableHandler TABLES_TABLE_HANDLER = VisualTableHandler.createByMySQL(createTablesTableSQL(), new Supplier<Observable<Object[]>>() {
        @Override
        public Observable<Object[]> get() {
            ArrayList<Object[]> resList = new ArrayList<>();
            String TABLE_CATALOG = "def";
            String TABLE_SCHEMA;
            String TABLE_NAME;
            String TABLE_TYPE = "BASE TABLE";
            String ENGINE = "InnoDB";
            long VERSION = 10;
            String ROW_FORMAT = "Compact";
            long TABLE_ROWS = 0;
            long AVG_ROW_LENGTH  = 0;
            long DATA_LENGTH  = 0;
            long MAX_DATA_LENGTH = 0;
            long INDEX_LENGTH = 0;
            long DATA_FREE = 0;
            long AUTO_INCREMENT = 0;
            LocalDateTime CREATE_TIME = null;
            LocalDateTime UPDATE_TIME = null;
            LocalDateTime CHECK_TIME = null;
            String TABLE_COLLATION = "utf8mb4_bin";
            long CHECKSUM = 0;
            String CREATE_OPTIONS = null;
            String TABLE_COMMENT = null;

            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            List<TableHandler> tables = metadataManager.getSchemaMap().entrySet().stream().flatMap(i -> i.getValue().logicTables().values().stream()).collect(Collectors.toList());
            for (TableHandler table : tables) {
                TABLE_SCHEMA = table.getSchemaName().toLowerCase();
                TABLE_NAME = table.getTableName().toLowerCase();

                resList.add(new Object[]{
                        TABLE_CATALOG,
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        TABLE_TYPE,
                        ENGINE,
                        VERSION,
                        ROW_FORMAT,
                        TABLE_ROWS,
                        AVG_ROW_LENGTH,
                        DATA_LENGTH,
                        MAX_DATA_LENGTH,
                        INDEX_LENGTH,
                        DATA_FREE,
                        AUTO_INCREMENT,
                        CREATE_TIME,
                        UPDATE_TIME,
                        CHECK_TIME,
                        TABLE_COLLATION,
                        CHECKSUM,
                        CREATE_OPTIONS,
                        TABLE_COMMENT
                });
            }

            return Observable.fromIterable(resList);
        }
    });


    public MysqlMetadataManager(Map<String, LogicSchemaConfig> schemaConfigs, PrototypeService prototypeService) {
        super(prototypeService);
        schemaMap.put("information_schema", createInformationSchema());
        schemaMap.put("performance_schema", createPerformanceSchema());
        schemaMap.put("mysql", createMysqlSchema());

        schemaConfigs.values().stream().parallel().forEach(c->{
            addSchema(c);
        });
    }


    private SchemaHandler createMysqlSchema() {
        SchemaHandlerImpl information_schema = new SchemaHandlerImpl("mysql", null);
        NameMap<TableHandler> tables = information_schema.logicTables();

        for (Map.Entry<String, String> stringStringEntry : MYSQL_SCHEMA_INFO.entrySet()) {
            String key = stringStringEntry.getKey();
            String value = stringStringEntry.getValue();

            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(value);
            if (sqlStatement instanceof MySqlCreateTableStatement) {
                VisualTableHandler visualTableHandler = VisualTableHandler.createByMySQL(value, new Supplier<Observable<Object[]>>() {

                    @Override
                    public Observable<Object[]> get() {
                        return Observable.empty();
                    }
                });
                tables.put(key, visualTableHandler);
            }else {
                System.out.println();
            }
        }
        return information_schema;
    }

    private SchemaHandler createPerformanceSchema() {
        SchemaHandlerImpl information_schema = new SchemaHandlerImpl("performance_schema", null);
        NameMap<TableHandler> tables = information_schema.logicTables();

        for (Map.Entry<String, String> stringStringEntry : PERFORMANCE_SCHEMA_INFO.entrySet()) {
            String key = stringStringEntry.getKey();
            String value = stringStringEntry.getValue();

            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(value);
            if (sqlStatement instanceof MySqlCreateTableStatement) {
                VisualTableHandler visualTableHandler = VisualTableHandler.createByMySQL(value, new Supplier<Observable<Object[]>>() {

                    @Override
                    public Observable<Object[]> get() {
                        return Observable.empty();
                    }
                });
                tables.put(key, visualTableHandler);
            }else {
                System.out.println();
            }
        }
        return information_schema;
    }

    private SchemaHandlerImpl createInformationSchema() {
        SchemaHandlerImpl information_schema = new SchemaHandlerImpl("information_schema", null);
        NameMap<TableHandler> tables = information_schema.logicTables();

        for (Map.Entry<String, String> stringStringEntry : INFORMATION_SCHEMA_INFO.entrySet()) {
            String key = stringStringEntry.getKey();
            String value = stringStringEntry.getValue();

            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(value);
            if (sqlStatement instanceof MySqlCreateTableStatement) {
                VisualTableHandler visualTableHandler = VisualTableHandler.createByMySQL(value, new Supplier<Observable<Object[]>>() {

                    @Override
                    public Observable<Object[]> get() {
                        return Observable.empty();
                    }
                });
                tables.put(key, visualTableHandler);
            }

        }

        Arrays.asList(EVENTS_TABLE_HANDLER,
                        CHARACTER_SETS_TABLE_HANDLER,
                        COLLATION_CHARACTER_SET_APPLICABILITY_TABLE_HANDLER,
                        COLLATIONS_TABLE_HANDLER,
                        COLUMN_STATISTICS_TABLE_HANDLER,
                        COLUMNS_TABLE_HANDLER,
                        FILE_TABLE_HANDLER,
                        INNODB_DATAFILES_TABLE_HANDLER,
                        INNODB_FIELDS_TABLE_HANDLER,
                        ENGINES_TABLE_HANDLER,
                        KEY_COLUMN_USAGE_TABLE_HANDLER,
                        PARTITIONS_TABLE_HANDLER,
                        PROCESSLIST_TABLE_HANDLER,
                        SCHEMATA_TABLE_HANDLER,
                        SESSION_VARIABLES_TABLE_HANDLER,
                        STATISTICS_TABLE_HANDLER,
                        TABLES_TABLE_HANDLER,
                        TABLES_CONSTRAINTS_TABLE_HANDLER,
                        VIEW_TABLE_HANDLER,
                        ROUTINES_TABLE_HANDLER,
                        GLOBAL_VARIABLES_TABLE_HANDLER,
                        TRIGGERS_TABLE_HANDLER)
                .forEach(c -> {
                    tables.put(c.getTableName().toUpperCase(), c);
                });
        return information_schema;
    }


    private static String createCHARACTER_SETSTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("CHARACTER_SETS");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("CHARACTER_SET_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("DEFAULT_COLLATE_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("DESCRIPTION", "varchar(60)");
        createEVENTSTableSQL.addColumn("MAXLEN", "bigint(3)");
        return createEVENTSTableSQL.toString();
    }

    private static String createSCHEMATATableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("SCHEMATA");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("CATALOG_NAME", "varchar(512)");
        createEVENTSTableSQL.addColumn("SCHEMA_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("DEFAULT_CHARACTER_SET_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("DEFAULT_COLLATION_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("SQL_PATH", "varchar(512)");
        return createEVENTSTableSQL.toString();
    }

    private static String createStatisticsTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("statistics");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("TABLE_CATALOG", "varchar(512)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("NON_UNIQUE", "varchar(1)");
        createEVENTSTableSQL.addColumn("INDEX_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("INDEX_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("SEQ_IN_INDEX", "bigint(2)");
        createEVENTSTableSQL.addColumn("COLUMN_NAME", "varchar(21)");
        createEVENTSTableSQL.addColumn("COLLATION", "varchar(1)");
        createEVENTSTableSQL.addColumn("CARDINALITY", "bigint(21)");
        createEVENTSTableSQL.addColumn("SUB_PART", "bigint(3)");
        createEVENTSTableSQL.addColumn("PACKED", "varchar(10)");
        createEVENTSTableSQL.addColumn("NULLABLE", "varchar(3)");
        createEVENTSTableSQL.addColumn("INDEX_TYPE", "varchar(16)");
        createEVENTSTableSQL.addColumn("COMMENT", "varchar(16)");
        createEVENTSTableSQL.addColumn("INDEX_COMMENT", "varchar(1024)");
        createEVENTSTableSQL.addColumn("IS_VISIBLE", "varchar(3)");
        createEVENTSTableSQL.addColumn("Expression", "varchar(64)");

        return createEVENTSTableSQL.toString();
    }


    private static String createEVENTSTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("EVENTS");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("EVENT_CATALOG", "varchar(64)");
        createEVENTSTableSQL.addColumn("EVENT_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("EVENT_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("DEFINER", "varchar(93)");
        createEVENTSTableSQL.addColumn("TIME_ZONE", "varchar(64)");
        createEVENTSTableSQL.addColumn("EVENT_BODY", "varchar(3)");
        createEVENTSTableSQL.addColumn("EVENT_DEFINITION", "longtext");
        createEVENTSTableSQL.addColumn("EVENT_TYPE", "varchar(9)");
        createEVENTSTableSQL.addColumn("EXECUTE_AT", "datetime");
        createEVENTSTableSQL.addColumn("INTERVAL_VALUE", "int(11)");
        createEVENTSTableSQL.addColumn("INTERVAL_FIELD", "varchar(255)");
        createEVENTSTableSQL.addColumn("SQL_MODE", "varchar(1024)");
        createEVENTSTableSQL.addColumn("STARTS", "datetime");
        createEVENTSTableSQL.addColumn("ENDS", "datetime");
        createEVENTSTableSQL.addColumn("STATUS", "varchar(1024)");
        createEVENTSTableSQL.addColumn("ON_COMPLETION", "varchar(1024)");
        createEVENTSTableSQL.addColumn("CREATED", "timestamp");
        createEVENTSTableSQL.addColumn("LAST_ALTERED", "timestamp");
        createEVENTSTableSQL.addColumn("LAST_EXECUTED", "datetime");
        createEVENTSTableSQL.addColumn("EVENT_COMMENT", "varchar(2048)");
        createEVENTSTableSQL.addColumn("ORIGINATOR", "int(10) unsigned");
        createEVENTSTableSQL.addColumn("CHARACTER_SET_CLIENT", "varchar(64)");
        createEVENTSTableSQL.addColumn("COLLATION_CONNECTION", "varchar(64)");
        createEVENTSTableSQL.addColumn("DATABASE_COLLATION", "varchar(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createCOLLATION_CHARACTER_SET_APPLICABILITYTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("COLLATION_CHARACTER_SET_APPLICABILITY");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("COLLATION_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("CHARACTER_SET_NAME", "varchar(32)");
        return createEVENTSTableSQL.toString();
    }

    private static String createCOLLATIONSTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("COLLATIONS");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("COLLATION_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("CHARACTER_SET_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("ID", "bigint(11)");
        createEVENTSTableSQL.addColumn("IS_DEFAULT", "varchar(3)");
        createEVENTSTableSQL.addColumn("IS_COMPILED", "varchar(3)");
        createEVENTSTableSQL.addColumn("SORTLEN", "bigint(3)");
        createEVENTSTableSQL.addColumn("PAD_ATTRIBUTE", "varchar(16)");
        return createEVENTSTableSQL.toString();
    }

    private static String createCOLUMN_STATISTICSTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("COLUMN_STATISTICS");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("SCHEMA_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("COLUMN_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("HISTOGRAM", "varchar(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createColumnsTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("COLUMNS");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("TABLE_CATALOG", "varchar(512)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("COLUMN_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("ORDINAL_POSITION", "bigint(64)");

        createEVENTSTableSQL.addColumn("COLUMN_DEFAULT", "text");
        createEVENTSTableSQL.addColumn("IS_NULLABLE", "varchar(3)");
        createEVENTSTableSQL.addColumn("DATA_TYPE", "varchar(64)");

        createEVENTSTableSQL.addColumn("CHARACTER_MAXIMUM_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("CHARACTER_OCTET_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("NUMERIC_PRECISION", "bigint(21)");
        createEVENTSTableSQL.addColumn("NUMERIC_SCALE", "bigint(21)");
        createEVENTSTableSQL.addColumn("DATETIME_PRECISION", "bigint(21)");

        createEVENTSTableSQL.addColumn("CHARACTER_SET_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("COLLATION_NAME", "varchar(32)");
        createEVENTSTableSQL.addColumn("COLUMN_TYPE", "text");
        createEVENTSTableSQL.addColumn("COLUMN_KEY", "varchar(3)");

        createEVENTSTableSQL.addColumn("EXTRA", "varchar(30)");
        createEVENTSTableSQL.addColumn("PRIVILEGES", "varchar(80)");
        createEVENTSTableSQL.addColumn("COLUMN_COMMENT", "varchar(1024)");
        createEVENTSTableSQL.addColumn("GENERATION_EXPRESSION", "text");
        return createEVENTSTableSQL.toString();
    }

    private static String createINNODB_BUFFER_PAGE_LRUTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("INNODB_BUFFER_PAGE_LRU");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("POOL_ID", "varchar(64)");
        createEVENTSTableSQL.addColumn("LRU_POSITION", "varchar(64)");
        createEVENTSTableSQL.addColumn("SPACE", "int(64)");
        createEVENTSTableSQL.addColumn("PAGE_NUMBER", "int(64)");
        createEVENTSTableSQL.addColumn("PAGE_TYPE", "varchar(64)");
        createEVENTSTableSQL.addColumn("FLUSH_TYPE", "int(64)");
        createEVENTSTableSQL.addColumn("FIX_COUNT", "int(64)");
        createEVENTSTableSQL.addColumn("IS_HASHED", "varchar(64)");
        createEVENTSTableSQL.addColumn("NEWEST_MODIFICATION", "int(64)");
        createEVENTSTableSQL.addColumn("OLDEST_MODIFICATION", "int(64)");
        createEVENTSTableSQL.addColumn("ACCESS_TIME", "int(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("INDEX_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("NUMBER_RECORDS", "int(64)");
        createEVENTSTableSQL.addColumn("DATA_SIZE", "int(64)");
        createEVENTSTableSQL.addColumn("COMPRESSED_SIZE", "int(64)");
        createEVENTSTableSQL.addColumn("COMPRESSED", "varchar(64)");
        createEVENTSTableSQL.addColumn("IO_FIX", "varchar(64)");
        createEVENTSTableSQL.addColumn("IO_OLD", "int(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createFileTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("FILES");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("FILE_ID", "varchar(64)");
        createEVENTSTableSQL.addColumn("FILE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("FILE_TYPE", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLESPACE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_CATALOG", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("LOGFILE_GROUP_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("LOGFILE_GROUP_NUMBER", "varchar(64)");
        createEVENTSTableSQL.addColumn("ENGINE", "varchar(64)");

        createEVENTSTableSQL.addColumn("DELETED_ROWS", "int(64)");
        createEVENTSTableSQL.addColumn("UPDATE_COUNT", "int(64)");
        createEVENTSTableSQL.addColumn("NEWEST_MODIFICATION", "int(64)");
        createEVENTSTableSQL.addColumn("OLDEST_MODIFICATION", "int(64)");
        createEVENTSTableSQL.addColumn("ACCESS_TIME", "int(64)");
        createEVENTSTableSQL.addColumn("INDEX_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("NUMBER_RECORDS", "int(64)");
        createEVENTSTableSQL.addColumn("DATA_SIZE", "int(64)");
        createEVENTSTableSQL.addColumn("COMPRESSED_SIZE", "int(64)");
        createEVENTSTableSQL.addColumn("COMPRESSED", "varchar(64)");
        createEVENTSTableSQL.addColumn("IO_FIX", "varchar(64)");
        createEVENTSTableSQL.addColumn("IO_OLD", "int(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createDATAFILESTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("DATAFILES");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("SPACE", "varchar(64)");
        createEVENTSTableSQL.addColumn("PATH", "varchar(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createINNODB_FIELDSTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("DATAFILES");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("INDEX_ID", "int(64)");
        createEVENTSTableSQL.addColumn("NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("POS", "int(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createKEY_COLUMN_USAGETableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("key_column_usage");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("CONSTRAINT_CATALOG", "varchar(512)");
        createEVENTSTableSQL.addColumn("CONSTRAINT_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("CONSTRAINT_NAME", " varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_CATALOG", "varchar(512)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("COLUMN_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("ORDINAL_POSITION", " bigint(10)");
        createEVENTSTableSQL.addColumn("POSITION_IN_UNIQUE_CONSTRAINT", "bigint(10)");
        createEVENTSTableSQL.addColumn("REFERENCED_TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("REFERENCED_TABLE_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("REFERENCED_COLUMN_NAME", "varchar(64)");
        return createEVENTSTableSQL.toString();
    }

    private static String createPARTITIONSTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("partitions");
        createEVENTSTableSQL.setSchema("information_schema");

        createEVENTSTableSQL.addColumn("TABLE_CATALOG", "varchar(512)");
        createEVENTSTableSQL.addColumn("TABLE_SCHEMA", "varchar(64)");
        createEVENTSTableSQL.addColumn("TABLE_NAME", " varchar(64)");
        createEVENTSTableSQL.addColumn("PARTITION_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("SUBPARTITION_NAME", "varchar(64)");
        createEVENTSTableSQL.addColumn("PARTITION_ORDINAL_POSITION", "bigint(21)");
        createEVENTSTableSQL.addColumn("SUBPARTITION_ORDINAL_POSITION ", "bigint(21)");

        createEVENTSTableSQL.addColumn("PARTITION_METHOD", "varchar(18)");
        createEVENTSTableSQL.addColumn("SUBPARTITION_METHOD", "varchar(12)");
        createEVENTSTableSQL.addColumn("PARTITION_EXPRESSION", "longblob");
        createEVENTSTableSQL.addColumn("SUBPARTITION_EXPRESSION", "longblob");
        createEVENTSTableSQL.addColumn("PARTITION_DESCRIPTION", "longblob");
        createEVENTSTableSQL.addColumn("TABLE_ROWS", "bigint(21)");
        createEVENTSTableSQL.addColumn("AVG_ROW_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("DATA_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("MAX_DATA_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("INDEX_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("DATA_FREE", "bigint(21)");

        createEVENTSTableSQL.addColumn("CREATE_TIME", "datetime");
        createEVENTSTableSQL.addColumn("UPDATE_TIME", "datetime");
        createEVENTSTableSQL.addColumn("CHECK_TIME", "datetime");

        createEVENTSTableSQL.addColumn("CHECKSUM", "bigint(21)");
        createEVENTSTableSQL.addColumn("PARTITION_COMMENT", "varchar(80)");
        createEVENTSTableSQL.addColumn("NODEGROUP", "varchar(12)");
        createEVENTSTableSQL.addColumn("TABLESPACE_NAME", "varchar(64)");

        return createEVENTSTableSQL.toString();
    }
    private static String createROUTINESTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("ROUTINES");
        createEVENTSTableSQL.setSchema("information_schema");

        createEVENTSTableSQL.addColumn("SPECIFIC_NAME", "varchar(192)");
        createEVENTSTableSQL.addColumn("ROUTINE_CATALOG", "varchar(192)");
        createEVENTSTableSQL.addColumn("ROUTINE_SCHEMA", " varchar(192)");
        createEVENTSTableSQL.addColumn("ROUTINE_NAME", "varchar(192)");
        createEVENTSTableSQL.addColumn("ROUTINE_TYPE", "varchar(27)");
        createEVENTSTableSQL.addColumn("DATA_TYPE", "varchar(1024)");
        createEVENTSTableSQL.addColumn("CHARACTER_MAXIMUM_LENGTH ", "bigint(21)");

        createEVENTSTableSQL.addColumn("CHARACTER_OCTET_LENGTH", "bigint(21)");
        createEVENTSTableSQL.addColumn("NUMERIC_PRECISION", "bigint(3)");
        createEVENTSTableSQL.addColumn("NUMERIC_SCALE", "bigint(3)");
        createEVENTSTableSQL.addColumn("DATETIME_PRECISION", "bigint(3)");

        createEVENTSTableSQL.addColumn("CHARACTER_SET_NAME", "varchar(12)");
        createEVENTSTableSQL.addColumn("COLLATION_NAME", "varchar(12)");
        createEVENTSTableSQL.addColumn("DTD_IDENTIFIER", "varchar(255)");
        createEVENTSTableSQL.addColumn("ROUTINE_BODY", "varchar(255)");
        createEVENTSTableSQL.addColumn("ROUTINE_DEFINITION", "varchar(255)");
        createEVENTSTableSQL.addColumn("EXTERNAL_NAME", "varchar(255)");
        createEVENTSTableSQL.addColumn("EXTERNAL_LANGUAGE", "varchar(255)");
        createEVENTSTableSQL.addColumn("PARAMETER_STYLE", "varchar(255)");
        createEVENTSTableSQL.addColumn("IS_DETERMINISTIC", "varchar(255)");
        createEVENTSTableSQL.addColumn("SQL_DATA_ACCESS", "char(64)");

        createEVENTSTableSQL.addColumn("SQL_PATH", "varchar(255)");
        createEVENTSTableSQL.addColumn("SECURITY_TYPE", "varchar(255)");
        createEVENTSTableSQL.addColumn("CREATED", "datetime");

        createEVENTSTableSQL.addColumn("LAST_ALTERED", "datetime");
        createEVENTSTableSQL.addColumn("SQL_MODE", "varchar(80)");
        createEVENTSTableSQL.addColumn("ROUTINE_COMMENT", "varchar(12)");
        createEVENTSTableSQL.addColumn("DEFINER", "varchar(64)");
        createEVENTSTableSQL.addColumn("CHARACTER_SET_CLIENT", "varchar(64)");
        createEVENTSTableSQL.addColumn("COLLATION_CONNECTION", "varchar(64)");
        createEVENTSTableSQL.addColumn("DATABASE_COLLATION", "varchar(64)");
        return createEVENTSTableSQL.toString();
    }
    private static String createTRIGGERSTableSQL() {
        MySqlCreateTableStatement createEVENTSTableSQL = new MySqlCreateTableStatement();
        createEVENTSTableSQL.setTableName("TRIGGERS");
        createEVENTSTableSQL.setSchema("information_schema");
        createEVENTSTableSQL.addColumn("TRIGGER_CATALOG", "varchar(192)");
        createEVENTSTableSQL.addColumn("TRIGGER_SCHEMA", "varchar(192)");
        createEVENTSTableSQL.addColumn("TRIGGER_NAME", "varchar(192)");
        createEVENTSTableSQL.addColumn("EVENT_MANIPULATION", "varchar(192)");
        createEVENTSTableSQL.addColumn("EVENT_OBJECT_CATALOG", "varchar(192)");
        createEVENTSTableSQL.addColumn("EVENT_OBJECT_SCHEMA", "varchar(192)");
        createEVENTSTableSQL.addColumn("EVENT_OBJECT_TABLE", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_ORDER", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_CONDITION", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_STATEMENT", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_ORIENTATION", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_TIMING", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_REFERENCE_OLD_TABLE", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_REFERENCE_NEW_TABLE", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_REFERENCE_OLD_ROW", "varchar(192)");
        createEVENTSTableSQL.addColumn("ACTION_REFERENCE_NEW_ROW", "varchar(192)");
        createEVENTSTableSQL.addColumn("CREATED", "datetime");
        createEVENTSTableSQL.addColumn("SQL_NAME", "varchar(192)");
        createEVENTSTableSQL.addColumn("SQL_MODE", "varchar(192)");
        createEVENTSTableSQL.addColumn("DEFINER", "varchar(192)");
        createEVENTSTableSQL.addColumn("CHARACTER_SET_CLIENT", "varchar(192)");
        createEVENTSTableSQL.addColumn("COLLATION_CONNECTION", "varchar(192)");
        createEVENTSTableSQL.addColumn("DATABASE_COLLATION", "varchar(192)");
        return createEVENTSTableSQL.toString();
    }
}
