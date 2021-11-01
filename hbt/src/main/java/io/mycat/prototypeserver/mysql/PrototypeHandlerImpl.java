package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.MysqlVariableService;
import io.mycat.TableHandler;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.util.NameMap;
import org.apache.doris.common.PatternMatcher;
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
    public List<Object[]> showDataBase(com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement sqlShowDatabasesStatement) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        SQLExpr like = sqlShowDatabasesStatement.getLike();
        List<String> collect = metadataManager.showDatabases();
        if (like != null) {
            PatternMatcher matcher = PatternMatcher.createMysqlPattern(SQLUtils.normalize(like.toString()),
                    false);
            collect = collect.stream().filter(i -> matcher.match(i)).collect(Collectors.toList());
        }
        return collect.stream().map(i -> new Object[]{i}).collect(Collectors.toList());
    }

    @Override
    public List<Object[]> showTables(SQLShowTablesStatement statement) {
        String schemaName = SQLUtils.normalize(statement.getDatabase().getSimpleName());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        NameMap<SchemaHandler> schemaMap = metadataManager.getSchemaMap();
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler == null) return Collections.emptyList();
        Collection<String> strings = schemaHandler.logicTables().values().stream().map(i -> i.getTableName()).collect(Collectors.toList());
        ;
        SQLExpr like = statement.getLike();
        if (like == null) {
            NameMap<TableHandler> tables = schemaHandler.logicTables();
            strings = tables.keySet();
        } else if (like instanceof SQLTextLiteralExpr) {
            PatternMatcher matcher = PatternMatcher.createMysqlPattern(((SQLTextLiteralExpr) like).getText(),
                    false);
            strings = strings.stream().filter(i -> matcher.match(i)).collect(Collectors.toList());
        }
        if (statement.isFull()) {
            return strings.stream().sorted().map(i -> new Object[]{i, "BASE TABLE"}).collect(Collectors.toList());
        } else {
            return strings.stream().sorted().map(i -> new Object[]{i}).collect(Collectors.toList());
        }
    }

    @Override
    public List<Object[]> showColumns(SQLShowColumnsStatement statement) {
        ArrayList<Object[]> objects = new ArrayList<>();
        String schemaName = SQLUtils.normalize(statement.getDatabase().getSimpleName());


        String tableName = Optional.ofNullable((SQLExpr) statement.getTable()).map(i -> SQLUtils.normalize(i.toString())).orElse(null);

        if (tableName == null && statement.getLike() != null) {
            String pattern = SQLUtils.normalize(statement.getLike().toString());
            PatternMatcher mysqlPattern = PatternMatcher.createMysqlPattern(pattern, false);
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            SchemaHandler schemaHandler = metadataManager.getSchemaMap().get(schemaName);
            if (schemaHandler == null) return Collections.emptyList();
            List<String> tables = schemaHandler.logicTables().values().stream().map(i -> i.getTableName()).collect(Collectors.toList());
            tableName = tables.stream().filter(i -> mysqlPattern.match(i)).findFirst().orElse(null);
        }
        if (tableName == null) {
            return Collections.emptyList();
        }

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
            String Collation = Optional.ofNullable(column.getCollateExpr()).map(s -> SQLUtils.normalize(s.toString())).orElse(null);
            String Null = column.containsNotNullConstaint() ? "NO" : "YES";
            String Key = sqlStatement.isPrimaryColumn(name) ?
                    "PRI" : sqlStatement.isUNI(name) ? "UNI" : sqlStatement.isMUL(name) ? "MUL" : "";

            String Default = Optional.ofNullable(defaultValues.get(i)).orElse("NULL");
            String Extra = "";
            String Privileges = "select,insert,update,references";
            String Comment = Optional.ofNullable(column.getComment()).map(s->s.toString()).orElse("");

            if (statement.isFull()){
                objects.add(new Object[]{Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment});
            }else {
                objects.add(new Object[]{Field, Type, Null, Key, Default, Extra});
            }

        }
        return objects;
    }

    @Override
    public List<Object[]> showTableStatus(MySqlShowTableStatusStatement statement) {
        String database = SQLUtils.normalize(statement.getDatabase().getSimpleName());
        SQLExpr like = statement.getLike();
        PatternMatcher matcher = PatternMatcher.createMysqlPattern(SQLUtils.normalize(like.toString()),
                false);
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        SchemaHandler schemaHandler = metadataManager.getSchemaMap().get(database);
        if (schemaHandler == null) return Collections.emptyList();
        return schemaHandler.logicTables().values().stream().map(i -> i.getTableName()).filter(i -> matcher.match(i)).map(tableName -> {
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
            return new Object[]{tableName,
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
            };
        }).collect(Collectors.toList());
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
        String tableName = SQLUtils.normalize(sqlPropertyExpr.getSimpleName());
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
            res.add(new Object[]{"utf8_general_ci", "utf8", "33", "Yes", "yes", "1", "PAD SPACE"});
            return res;
        });
    }

    @Override
    public List<Object[]> showStatus(MySqlShowStatusStatement statement) {
        return onJdbc(statement.toString()).orElseGet(() -> Collections.emptyList());
    }

    @Override
    public List<Object[]> showCreateFunction(MySqlShowCreateFunctionStatement statement) {
        return onJdbc(statement.toString()).orElseGet(() -> Collections.emptyList());
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
        return onJdbc(statement.toString()).orElseGet(() -> Collections.emptyList());
    }

    @Override
    public List<Object[]> showProcedureStatus(MySqlShowProcedureStatusStatement statement) {
        return onJdbc(statement.toString()).orElseGet(() -> Collections.emptyList());
    }

    @Override
    public List<Object[]> showVariants(MySqlShowVariantsStatement statement) {
        return onJdbc(statement.toString()).orElseGet(() -> {
            if (MetaClusterCurrent.exist(MysqlVariableService.class)) {
                MysqlVariableService mysqlVariableService = MetaClusterCurrent.wrapper(MysqlVariableService.class);
                List<Object[]> globalVariables = Collections.emptyList();
                List<Object[]> sessionVariables = Collections.emptyList();
                if (statement.isGlobal()) {
                    globalVariables = mysqlVariableService.getGlobalVariables();
                }
                if (statement.isSession()) {
                    sessionVariables = mysqlVariableService.getSessionVariables();
                }
                Set<String> variableKeys = new HashSet<>();
                List<Object[]> resVariables = new ArrayList<>(globalVariables.size() + sessionVariables.size());
                resVariables.addAll(globalVariables);
                for (Object[] globalVariable : globalVariables) {
                    variableKeys.add(Objects.toString(globalVariable[0]));
                }
                for (Object[] sessionVariable : sessionVariables) {
                    String key = Objects.toString(sessionVariable[0]);
                    if (variableKeys.add(key)) {
                        resVariables.add(sessionVariable);
                    }
                }
                return resVariables;
            }
            return Collections.emptyList();
        });
    }

    @Override
    public List<Object[]> showWarnings(MySqlShowWarningsStatement statement) {
        return Collections.emptyList();
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
        if (datasourceDs == null) {
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
