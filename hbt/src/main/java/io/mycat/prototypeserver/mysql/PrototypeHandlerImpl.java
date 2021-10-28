package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabaseStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.TableHandler;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.util.NameMap;

import java.util.*;
import java.util.stream.Collectors;

public class PrototypeHandlerImpl implements PrototypeHandler {
    public static final PrototypeHandler INSTANCE = new PrototypeHandlerImpl();

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
        if (like== null){
            SchemaHandler schemaHandler = schemaMap.get(schemaName);
            if (schemaHandler == null) return Collections.emptyList();
            NameMap<TableHandler> tables = schemaHandler.logicTables();
            strings = tables.keySet();
        }else {
            TableHandler table = metadataManager.getTable(schemaName, SQLUtils.normalize(like.toString()));
            if (table==null)return Collections.emptyList();
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
}
