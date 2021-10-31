package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.TableHandler;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.util.NameMap;
import io.vertx.core.json.Json;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PrototypeHandlerImpl implements PrototypeHandler {
    public static final PrototypeHandler INSTANCE = new PrototypeHandlerImpl();
    private static final Logger LOGGER = LoggerFactory.getLogger(PrototypeHandlerImpl.class);

    @Override
    public List<Object[]> showDataBase(MySqlShowDatabaseStatusStatement mySqlShowDatabaseStatusStatement) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        List<String> strings = metadataManager.showDatabases();
        return strings.stream().map(i -> new Object[]{i}).collect(Collectors.toList());
    }

    @Override
    public List<Object[]> showTables(SQLShowTablesStatement statement) {
        String schemaName = SQLUtils.normalize(statement.getDatabase().getSimpleName());

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        NameMap<SchemaHandler> schemaMap = metadataManager.getSchemaMap();
        Collection<String> strings;
        SQLExpr like = statement.getLike();
        if (like == null) {
            SchemaHandler schemaHandler = schemaMap.get(schemaName);
            if (schemaHandler == null) return Collections.emptyList();
            NameMap<TableHandler> tables = schemaHandler.logicTables();
            strings = tables.keySet();
        } else {
            TableHandler table = metadataManager.getTable(schemaName, SQLUtils.normalize(like.toString()));
            if (table == null) return Collections.emptyList();
            strings = Collections.singleton(table.getTableName());
        }
        strings = strings.stream().sorted().collect(Collectors.toList());

        return strings.stream().map(i -> new Object[]{i, "BASE TABLE"}).collect(Collectors.toList());
    }

    @Override
    public List<Object[]> showColumns(SQLShowColumnsStatement statement) {
        ArrayList<Object[]> objects = new ArrayList<>();
        String schemaName = SQLUtils.normalize(statement.getDatabase().getSimpleName());
        String tableName = Optional.ofNullable((SQLExpr) statement.getTable()).orElse(statement.getLike()).toString();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler table = metadataManager.getTable(schemaName, tableName);
        if (table == null) return Collections.emptyList();
        String createTableSQL = table.getCreateTableSQL();
        MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSQL);


        List<SQLColumnDefinition> columns = new ArrayList<SQLColumnDefinition>();
        List<String> dataTypes = new ArrayList<String>();
        List<String> defaultValues = new ArrayList<String>();

        int name_len = -1, dataType_len = -1, defaultVal_len = 7, extra_len = 5;
        for (SQLTableElement element : sqlStatement.getTableElementList()) {
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

        for (int i = 0; i < columns.size(); i++) {
            SQLColumnDefinition column = columns.get(i);
            String name = SQLUtils.normalize(column.getName().getSimpleName());

            String Field = name;
            String Type = dataTypes.get(i);
            String Collation = SQLUtils.normalize(column.getCollateExpr().toString());
            String Null = column.containsNotNullConstaint() ? "NO" : "YES";
            String Key = sqlStatement.isPrimaryColumn(name) ?
                    "PRI" : sqlStatement.isUNI(name) ? "UNI" : sqlStatement.isMUL(name) ? "MUL" : "";

            String Default = Optional.ofNullable(defaultValues.get(i)).orElse("NULL");
            String Extra = "";
            String Privileges = "select,insert,update,references";
            String Comment = column.getComment().toString();

            objects.add(new Object[]{Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment});
        }
        return objects;
    }

    @Override
    public List<Object[]> showTableStatus(MySqlShowTableStatusStatement statement) {
        ArrayList<Object[]> objects = new ArrayList<>();
        String database = SQLUtils.normalize(statement.getDatabase().getSimpleName());
        SQLExpr like = statement.getLike();
        if (like instanceof SQLLiteralExpr) {
            String tableName = SQLUtils.normalize(like.toString());

            String Name = tableName;
            String Engine = "InnoDB";
            String Version = "10";
            String Row_format = "Dynamic";
            String Rows = "0";
            String Avg_row_length = "0";
            String Data_length = "800000";
            String Max_data_length = "0";
            String Index_length = "800000";
            String Data_free = "0";
            String Auto_increment = null;
            String Create_time = "2021-10-26 14:32:03";
            String Update_time = null;
            String Check_time = null;
            String Collation = "utf8_general_ci";
            String Checksum = null;
            String Create_options = null;
            String Comment = "";
            objects.add(new Object[]{tableName,
                    Name,
                    Engine,
                    Version,
                    Row_format,
                    Rows,
                    Avg_row_length,
                    Data_length,
                    Max_data_length,
                    Index_length,
                    Data_free,
                    Auto_increment,
                    Create_time,
                    Update_time,
                    Check_time,
                    Collation,
                    Checksum,
                    Create_options,
                    Comment
            });
            return objects;
        }
        return objects;
    }

    @Override
    public List<Object[]> showCreateDatabase(MySqlShowCreateDatabaseStatement statement) {
        String database = SQLUtils.normalize(statement.getDatabase().toString());
        SQLCreateDatabaseStatement sqlCreateDatabaseStatement = new SQLCreateDatabaseStatement();
        sqlCreateDatabaseStatement.setDatabase(database);
        ArrayList<Object[]> objects = new ArrayList<>();
        objects.add(new Object[]{"database", sqlCreateDatabaseStatement.toString()});
        return objects;
    }

    @Override
    public List<Object[]> showCreateTable(SQLShowCreateTableStatement statement) {
        SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) statement.getName();
        String schemaName = SQLUtils.normalize(sqlPropertyExpr.getOwnerName());
        String tableName = SQLUtils.normalize(sqlPropertyExpr.getOwnerName());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        String createTableSQL = tableHandler.getCreateTableSQL();
        ArrayList<Object[]> objects = new ArrayList<>();
        objects.add(new Object[]{tableName, createTableSQL});
        return objects;
    }

    @Override
    public List<Object[]> showCharacterSet(MySqlShowCharacterSetStatement statement) {
        return onJdbc(statement.toString()).orElseGet(() -> {
            List<Object[]> res = new ArrayList<>();
            res.add(new Object[]{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", "4"});
            return res;
        });
    }

    @Override
    public List<Object[]> showCollation(MySqlShowCollationStatement statement) {
        return onJdbc(statement.toString()).orElseGet(() -> {
            List<Object[]> res = new ArrayList<>();
            res.add(new Object[]{"utf8_unicode_ci", "utf8", "192", "", "yes", "8", "PAD SPACE"});
            return res;
        });
    }

    @Override
    public List<Object[]> showStatus(MySqlShowStatusStatement statement) {
        return onJdbc(statement.toString()).orElseGet(() -> Collections.emptyList());
    }

    @Override
    public List<Object[]> showCreateFunction(MySqlShowCreateFunctionStatement statement) {
        return Collections.emptyList();
    }

    @Override
    public List<Object[]> showEngine(MySqlShowEnginesStatement statement) {
        ArrayList<List<Object>> objects = new ArrayList<>();
        objects.add(Arrays.asList("InnoDB", "DRFAULT", "Supports transactions, row-level locking, foreign keys and encryption for tables"));
        objects.add(Arrays.asList("CSV", "YES", "Stores tables as CSV files"));
        objects.add(Arrays.asList("MRG_MyISAM", "YES", "Collection of identical MyISAM tables"));
        objects.add(Arrays.asList("MEMORY", "YES", "Hash based, stored in memory, useful for temporary tables"));
        objects.add(Arrays.asList("MyISAM", "YES", "Non-transactional engine with good performance and small data footprint"));
        objects.add(Arrays.asList("SEQUENCE", "YES", "Generated tables filled with sequential values"));
        objects.add(Arrays.asList("Aria", "YES", "Crash-safe tables with MyISAM heritage"));
        objects.add(Arrays.asList("PERFORMANCE_SCHEMA", "YES", "Performance Schema"));

        return objects.stream().map(i -> i.toArray()).collect(Collectors.toList());
    }

    @Override
    public List<Object[]> showErrors(MySqlShowErrorsStatement statement) {
        return Collections.emptyList();
    }

    @Override
    public List<Object[]> showIndexesColumns(SQLShowIndexesStatement statement) {
        return Collections.emptyList();
    }

    @Override
    public List<Object[]> showProcedureStatus(MySqlShowProcedureStatusStatement statement) {
        return Collections.emptyList();
    }

    @Override
    public List<Object[]> showVariants(MySqlShowVariantsStatement statement) {
        Optional<List<Object[]>> objects = onJdbc(statement.toString());
        List<Object[]> objects1 = objects.get();
        String encode = Json.encode(objects1);
        List list = Json.decodeValue(encode, List.class);
        return objects1;
    }

    @Override
    public List<Object[]> showWarnings(MySqlShowWarningsStatement statement) {
        return null;
    }


    private Optional<List<Object[]>> onJdbc(String statement) {
        String datasourceDs = null;

        if (MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
            ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            datasourceDs = replicaSelectorManager.getDatasourceNameByReplicaName(MetadataManager.getPrototype(), true, null);
        } else {
            datasourceDs = "prototypeDs";
        }

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        Map<String, JdbcDataSource> datasourceInfo = jdbcConnectionManager.getDatasourceInfo();
        if (datasourceInfo.containsKey(datasourceDs)) {
            datasourceDs = null;
        } else {
            List<DatasourceConfig> configAsList = jdbcConnectionManager.getConfigAsList();
            if (!configAsList.isEmpty()) {
                datasourceDs = configAsList.get(0).getName();
            } else {
                datasourceDs = null;
            }
        }
        if (datasourceDs == null) {
            datasourceDs = datasourceInfo.values().stream().filter(i -> i.isMySQLType()).map(i -> i.getName()).findFirst().orElse(null);
        }
        if (datasourceDs == null){
            return Optional.empty();
        }
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(datasourceDs)) {
            Connection rawConnection = connection.getRawConnection();
            Statement jdbcStatement1 = rawConnection.createStatement();
            ResultSet resultSet = jdbcStatement1.executeQuery(statement);
            int columnCount = resultSet.getMetaData().getColumnCount();
            List<Object[]> res = new ArrayList<>();
            while (resultSet.next()) {
                Object[] objects = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    objects[i] = resultSet.getObject(i + 1);
                }
                res.add(objects);
            }
            return Optional.of(res);
        } catch (Exception e) {
            LOGGER.warn("", e);
        }
        return Optional.empty();
    }

}
