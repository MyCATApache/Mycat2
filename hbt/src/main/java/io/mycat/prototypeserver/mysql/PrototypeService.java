package io.mycat.prototypeserver.mysql;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabaseStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.github.jinahya.database.metadata.bind.*;
import io.mycat.BackendTableInfo;
import io.mycat.MetaClusterCurrent;
import io.mycat.Partition;
import io.mycat.SimpleColumnInfo;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.mycat.config.NormalBackEndTableInfoConfig;
import io.mycat.config.NormalTableConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.CalciteConvertors;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;
import io.mycat.util.SQL2ResultSetUtil;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 *
 */
public class PrototypeService {
    public final static String PROTOTYPE = "prototype";
    private static final Logger LOGGER = LoggerFactory.getLogger(PrototypeService.class);
    private final NameMap<NameMap<String>> schemaList;
    private ConnectionFactory connectionFactory;
    private PrototypeHandler prototypeHandler = new PrototypeHandlerImpl();

    public static interface ConnectionFactory {
        public List<DefaultConnection> scanConnections();
    }

    @SneakyThrows
    public PrototypeService(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        Map<String, Map<String, String>> router = getTables();
        this.schemaList = toNameMap(router);
    }


    public Optional<MySQLResultSet> handleSql(String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        return handleSql(sqlStatement);
    }

    @NotNull
    public Optional<MySQLResultSet> handleSql(SQLStatement sqlStatement) {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("prototype process sql:{}",sqlStatement);
        }
        if (sqlStatement instanceof MySqlShowDatabaseStatusStatement) {
            MySqlShowDatabaseStatusStatement mySqlShowDatabaseStatusStatement = (MySqlShowDatabaseStatusStatement) sqlStatement;
            MySQLResultSet mySQLResultSet = MySQLResultSet.create(getShowDatabasesColumns());
            mySQLResultSet.setRows(prototypeHandler.showDataBase(mySqlShowDatabaseStatusStatement));
            return Optional.of( mySQLResultSet);
        }
        if (sqlStatement instanceof SQLShowTablesStatement) {
            SQLShowTablesStatement statement = (SQLShowTablesStatement) sqlStatement;
            String database = SQLUtils.normalize(statement.getDatabase().getSimpleName());
            List<ColumnDefPacket> columnDefPacketList = getShowTablesColumns(database);
            MySQLResultSet mySQLResultSet = MySQLResultSet.create(columnDefPacketList);
            mySQLResultSet.setRows(prototypeHandler.showTables(statement));
            return Optional.of(mySQLResultSet);
        }
        if (sqlStatement instanceof SQLShowColumnsStatement) {
            SQLShowColumnsStatement statement = (SQLShowColumnsStatement) sqlStatement;
            String database = SQLUtils.normalize(statement.getDatabase().getSimpleName());
            String table = SQLUtils.normalize(statement.getTable().getSimpleName());
            List<ColumnDefPacket> columnDefPacketList = getShowColumnsColumns();
            MySQLResultSet mySQLResultSet = MySQLResultSet.create(columnDefPacketList);
            mySQLResultSet.setRows(prototypeHandler.showColumns(statement));
            return Optional.of(mySQLResultSet);
        }
        if (sqlStatement instanceof MySqlShowTableStatusStatement) {
            MySqlShowTableStatusStatement statement = (MySqlShowTableStatusStatement) sqlStatement;
            String database = SQLUtils.normalize(statement.getDatabase().getSimpleName());

            List<ColumnDefPacket> columnDefPacketList = getShowTableStatusColumns();
            MySQLResultSet mySQLResultSet = MySQLResultSet.create(columnDefPacketList);
            mySQLResultSet.setRows(prototypeHandler.showTableStatus(statement));
            return Optional.of(mySQLResultSet);
        }
        if (sqlStatement instanceof MySqlShowCreateDatabaseStatement) {
            MySqlShowCreateDatabaseStatement statement = (MySqlShowCreateDatabaseStatement) sqlStatement;

            List<ColumnDefPacket> columnDefPacketList = getShowTableStatusColumns();
            MySQLResultSet mySQLResultSet = MySQLResultSet.create(columnDefPacketList);
            mySQLResultSet.setRows(prototypeHandler.showCreateDatabase(statement));
            return Optional.of(mySQLResultSet);
        }
        if (sqlStatement instanceof SQLShowCreateTableStatement) {
            SQLShowCreateTableStatement statement = (SQLShowCreateTableStatement) sqlStatement;
            List<ColumnDefPacket> columnDefPacketList = getShowCreateTableColumns();
            MySQLResultSet mySQLResultSet = MySQLResultSet.create(columnDefPacketList);
            mySQLResultSet.setRows(prototypeHandler.showCreateTable(statement));
            return Optional.of(mySQLResultSet);
        }
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("prototype ignored sql:{}",sqlStatement);
        }
        // SELECT `TABLE_NAME` FROM `INFORMATION_SCHEMA`.`TABLES` WHERE `TABLE_SCHEMA` = '1cloud_0' AND `TABLE_TYPE` = 'VIEW';
        return Optional.empty();
    }

    private List<ColumnDefPacket> getShowCreateTableColumns() {
        ArrayList<ColumnDefPacket> columnDefPackets = new ArrayList<>();

        String Catalog = "def";
        String Database = "";
        String Table = "";
        String OriginalTable = "";
        String Name = "Table";
        String OriginalName = Name;
        int CharsetNumber = 33;
        int Length = 192;
        int Type = 253;
        int Flags = 0;
        byte Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


         Catalog = "def";
         Database = "";
         Table = "";
         OriginalTable = "";
         Name = "Create Table";
         OriginalName = Name;
         CharsetNumber = 33;
         Length = 3072;
         Type = 253;
         Flags = 1;
         Decimals = 31;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        return null;
    }

    private List<ColumnDefPacket> getShowTableStatusColumns() {
        ArrayList<ColumnDefPacket> columnDefPackets = new ArrayList<>();

        String Catalog = "def";
        String Database = "";
        String Table = "TABLES";
        String OriginalTable = "";
        String Name = "Name";
        String OriginalName = Name;
        int CharsetNumber = 33;
        int Length = 192;
        int Type = 253;
        int Flags = 0;
        byte Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


         Catalog = "def";
         Database = "";
         Table = "TABLES";
         OriginalTable = "";
         Name = "Engine";
         OriginalName = Name;
         CharsetNumber = 33;
         Length = 192;
         Type = 253;
         Flags = 0;
         Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Engine";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 192;
        Type = 253;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Version";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 2;
        Type = 3;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "mysql";
        Table = "TABLES";
        OriginalTable = "tables";
        Name = "Row_format";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 30;
        Type = 254;
        Flags = 0x0180;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "mysql";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Rows";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0x0020;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Avg_row_length";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0x0020;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Data_length";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0x0020;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));



        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Max_data_length";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0x0020;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Index_length";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0x0020;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Data_free";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0x0020;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Auto_increment";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0x0020;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));



        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "tables";
        Name = "Create_time";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 19;
        Type = 7;
        Flags = 0x1081;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "tables";
        Name = "Update_time";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 19;
        Type = 12;
        Flags = 0x0080;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "tables";
        Name = "Check_time";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 19;
        Type = 12;
        Flags = 0x0080;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "mysql";
        Table = "TABLES";
        OriginalTable = "collations";
        Name = "Collation";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 192;
        Type = 253;
        Flags = 0x1000;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Checksum";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Create_options";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 768;
        Type = 253;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "TABLES";
        OriginalTable = "";
        Name = "Comment";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 768;
        Type = 253;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        return columnDefPackets;

    }

    private List<Object[]> showColumns(SQLShowColumnsStatement statement) {
        return null;
    }

    private List<ColumnDefPacket> getShowColumnsColumns() {
        ArrayList<ColumnDefPacket> columnDefPackets = new ArrayList<>();

        String Catalog = "def";
        String Database = "";
        String Table = "SHOW_STATISTICS";
        String OriginalTable = "";
        String Name = "Table";
        String OriginalName = Name;
        int CharsetNumber = 33;
        int Length = 192;
        int Type = 253;
        int Flags = 0;
        byte Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Non_unique";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 1;
        Type = 3;
        Flags = 1;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Key_name";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 192;
        Type = 253;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "mysql";
        Table = "SHOW_STATISTICS";
        OriginalTable = "index_column_usage";
        Name = "Seq_in_index";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 10;
        Type = 3;
        Flags = 0x1021;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Column_name";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 192;
        Type = 253;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Collation";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 3;
        Type = 253;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));



        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Cardinality";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Sub_part";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Sub_part";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 21;
        Type = 8;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "";
        OriginalTable = "";
        Name = "Packed";
        OriginalName = Name;
        CharsetNumber = 63;
        Length = 0;
        Type = 6;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Null";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 9;
        Type = 253;
        Flags = 1;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Index_type";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 33;
        Type = 253;
        Flags = 81;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Comment";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 24;
        Type = 253;
        Flags = 1;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "indexes";
        Name = "Index_comment";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 6144;
        Type = 253;
        Flags = 0x1081;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Visible";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 9;
        Type = 253;
        Flags = 0x0001;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "";
        Table = "SHOW_STATISTICS";
        OriginalTable = "";
        Name = "Visible";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 9;
        Type = 253;
        Flags = 0x0001;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));


        return columnDefPackets;
    }

    private List<Object[]> showTables(SQLShowTablesStatement statement) {
        return Collections.emptyList();
    }

    private List<ColumnDefPacket> getShowTablesColumns(String database) {
        ArrayList<ColumnDefPacket> columnDefPackets = new ArrayList<>();

        String Catalog = "def";
        String Database = "";
        String Table = "TABLES";
        String OriginalTable = "";
        String Name = "Tables_in_" + database;
        String OriginalName = Name;
        int CharsetNumber = 33;
        int Length = 192;
        int Type = 253;
        int Flags = 0;
        byte Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        Catalog = "def";
        Database = "mysql";
        Table = "TABLES";
        OriginalTable = "tables";
        Name = "Tables_type";
        OriginalName = Name;
        CharsetNumber = 33;
        Length = 33;
        Type = 254;
        Flags = 0;
        Decimals = 0;

        columnDefPackets.add(createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals));

        return columnDefPackets;
    }

    @NotNull
    private List<ColumnDefPacket> getShowDatabasesColumns() {
        String Catalog = "def";
        String Database = "";
        String Table = "SCHEMATA";
        String OriginalTable = "";
        String Name = "Database";
        String OriginalName = "Database";
        int CharsetNumber = 33;
        int Length = 192;
        int Type = 253;
        int Flags = 0;
        byte Decimals = 0;

        ColumnDefPacket column = createColumn(Catalog, Database, Table, OriginalTable, Name, OriginalName, CharsetNumber, Length, Type, Flags, Decimals);
        return Arrays.asList(column);
    }


    @NotNull
    private ColumnDefPacket createColumn(String Catalog, String Database, String Table, String OriginalTable, String Name, String OriginalName, int CharsetNumber, int Length, int Type, int Flags, byte Decimals) {
        ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl();
        columnDefPacket.setColumnCatalog(Catalog.getBytes());
        columnDefPacket.setColumnSchema(Database.getBytes());
        columnDefPacket.setColumnTable(Table.getBytes());
        columnDefPacket.setColumnOrgTable(OriginalTable.getBytes());
        columnDefPacket.setColumnName(Name.getBytes());
        columnDefPacket.setColumnOrgName(OriginalName.getBytes());
        columnDefPacket.setColumnCharsetSet(CharsetNumber);
        columnDefPacket.setColumnLength(Length);
        columnDefPacket.setColumnType(Type);
        columnDefPacket.setColumnFlags(Flags);
        columnDefPacket.setColumnDecimals(Decimals);
        return columnDefPacket;
    }

    @NotNull
    private NameMap<NameMap<String>> toNameMap(Map<String, Map<String, String>> router) {
        HashMap<String, NameMap> map = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : router.entrySet()) {
            String key = entry.getKey();
            NameMap nameMap = NameMap.immutableCopyOf(entry.getValue());
            map.put(key, nameMap);
        }
        return (NameMap<NameMap<String>>) NameMap.immutableCopyOf(map);
    }

    static class Info {
        List<Catalog> catalogList;
        String name;
    }

    @Nullable
    private Map<String, Map<String, String>> getTables() {
        List<DefaultConnection> connections = this.connectionFactory.scanConnections();
        try {
            Map<String, Map<String, String>> router = new HashMap<>();
            Iterator<Info> iterator = connections.stream().parallel().map(connection -> {
                        try {
                            Context context = Context.newInstance(connection.getRawConnection())
                                    .suppress(SQLFeatureNotSupportedException.class);
                            Info info = new Info();
                            info.catalogList = context.getCatalogs(new ArrayList<>());
                            info.name = connection.getDataSource().getName();
                            return info;
                        } catch (Throwable ignored) {
                            return null;
                        }
                    })
                    .filter(i -> i != null).iterator();
            while (iterator.hasNext()) {
                Info catalog = iterator.next();
                for (Catalog catalog1 : catalog.catalogList) {
                    List<Schema> schemas = catalog1.getSchemas();
                    for (Schema schema : schemas) {
                        String tableCatalog = schema.getTableCatalog();
                        Map<String, String> tableMap = router.computeIfAbsent(tableCatalog, s -> new HashMap<>());
                        List<Table> tables = schema.getTables();
                        for (Table table : tables) {
                            String tableName = table.getTableName();
                            MySqlCreateTableStatement mySqlCreateTableStatement = new MySqlCreateTableStatement();
                            mySqlCreateTableStatement.setTableName(tableName);
                            mySqlCreateTableStatement.setSchema(tableCatalog);
                            List<Column> columns = table.getColumns();
                            int columnCount = columns.size();
                            Optional<PrimaryKey> primaryKeyOptional = table.getPrimaryKeys().stream().findFirst();
                            for (int i = 0; i < columnCount; i++) {
                                Column column = columns.get(i);
                                String columnName = column.getColumnName();
                                int columnType = column.getDataType();
                                String type = SQLDataType.Constants.VARCHAR;
                                for (MySQLType value : MySQLType.values()) {
                                    if (value.getJdbcType() == columnType) {
                                        type = value.getName();
                                    }
                                }
                                boolean nullable = column.getNullable() != ResultSetMetaData.columnNoNulls;
                                int columnSize = column.getColumnSize();


                                SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition();
                                sqlColumnDefinition.setName(columnName);
                                sqlColumnDefinition.setDataType(SQLParserUtils.createExprParser(type, DbType.mysql).parseDataType());
                                if (!nullable) {
                                    sqlColumnDefinition.addConstraint(new SQLNullConstraint());
                                }
                                if (primaryKeyOptional.isPresent()) {
                                    if (primaryKeyOptional.get().getColumnName().equalsIgnoreCase(columnName)) {
                                        sqlColumnDefinition.addConstraint(new SQLColumnPrimaryKey());
                                    }
                                }
                                mySqlCreateTableStatement.addColumn(column.getColumnName(), type);
                            }
                            tableMap.put(tableName, mySqlCreateTableStatement.toString());
                        }
                    }
                }
            }
            return router;
        }finally {
            close(connections);
        }
    }

    public static void close(Iterable<DefaultConnection> ac) {
        if (ac != null) {
            for (DefaultConnection connection : ac) {
                connection.close();
            }
        }
    }

    public Optional<JdbcConnectionManager> getPrototypeConnectionManager() {
        if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
            return Optional.of(MetaClusterCurrent.wrapper(JdbcConnectionManager.class));
        }
        return Optional.empty();
    }


    public Optional<String> getCreateTableSQLByJDBC(String schemaName, String tableName, List<Partition> backends) {
        Optional<JdbcConnectionManager> jdbcConnectionManagerOptional = getPrototypeConnectionManager();
        if (!jdbcConnectionManagerOptional.isPresent()) {
            return Optional.empty();
        }
        JdbcConnectionManager jdbcConnectionManager = jdbcConnectionManagerOptional.get();
        backends = new ArrayList<>(backends);
        backends.add(new BackendTableInfo(PROTOTYPE, schemaName, tableName));

        if (backends == null || backends.isEmpty()) {
            return null;
        }
        for (Partition backend : backends) {
            try {
                Partition backendTableInfo = backend;
                String targetName = backendTableInfo.getTargetName();
                String targetSchemaTable = backendTableInfo.getTargetSchemaTable();
                try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
                    String sql = "SHOW CREATE TABLE " + targetSchemaTable;
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql)) {
                        while (rowBaseIterator.next()) {
                            String string = rowBaseIterator.getString(1);
                            SQLStatement sqlStatement = null;
                            try {
                                sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                            } catch (Throwable e) {

                            }
                            if (sqlStatement == null) {
                                try {
                                    string = string.substring(0, string.lastIndexOf(')') + 1);
                                    sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                                } catch (Throwable e) {

                                }
                            }
                            if (sqlStatement instanceof MySqlCreateTableStatement) {
                                MySqlCreateTableStatement sqlStatement1 = (MySqlCreateTableStatement) sqlStatement;

                                sqlStatement1.setTableName(SQLUtils.normalize(tableName));
                                sqlStatement1.setSchema(SQLUtils.normalize(schemaName));//顺序不能颠倒
                                return Optional.of(sqlStatement1.toString());
                            }
                            if (sqlStatement instanceof SQLCreateViewStatement) {
                                SQLCreateViewStatement sqlStatement1 = (SQLCreateViewStatement) sqlStatement;
                                SQLExprTableSource sqlExprTableSource = sqlStatement1.getTableSource();
                                if (!SQLUtils.nameEquals(sqlExprTableSource.getTableName(), tableName) ||
                                        !SQLUtils.nameEquals(sqlExprTableSource.getSchema(), (schemaName))) {
                                    MycatSQLExprTableSourceUtil.setSqlExprTableSource(schemaName, tableName, sqlExprTableSource);
                                    return Optional.of(sqlStatement1.toString());
                                } else {
                                    return Optional.of(string);
                                }
                            }

                        }
                    } catch (Exception e) {
                        LOGGER.error("", e);
                    }
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery("select * from " + targetSchemaTable + " where 0 limit 0")) {
                        MycatRowMetaData metaData = rowBaseIterator.getMetaData();
                        MySqlCreateTableStatement mySqlCreateTableStatement = new MySqlCreateTableStatement();
                        mySqlCreateTableStatement.setTableName(tableName);
                        mySqlCreateTableStatement.setSchema(schemaName);
                        int columnCount = metaData.getColumnCount();
                        for (int i = 0; i < columnCount; i++) {
                            int columnType = metaData.getColumnType(i);
                            String type = SQLDataType.Constants.VARCHAR;
                            for (MySQLType value : MySQLType.values()) {
                                if (value.getJdbcType() == columnType) {
                                    type = value.getName();
                                }
                            }
                            mySqlCreateTableStatement.addColumn(metaData.getColumnName(i), type);
                        }
                        return Optional.of(mySqlCreateTableStatement.toString());

                    }
                }
            } catch (Throwable e) {
                LOGGER.error("can not get create table sql from:" + backend.getTargetName() + backend.getTargetSchemaTable(), e);
                continue;
            }
        }
        return Optional.empty();
    }

    public List<SimpleColumnInfo> getColumnInfo(String sql) {
        String prototypeServer = PROTOTYPE;
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MycatRowMetaData mycatRowMetaData = null;
        if (sqlStatement instanceof MySqlCreateTableStatement) {
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData((MySqlCreateTableStatement) sqlStatement);
        }
        if (sqlStatement instanceof SQLCreateViewStatement) {
            Optional<JdbcConnectionManager> prototypeConnectionManagerOptional = getPrototypeConnectionManager();
            if (!prototypeConnectionManagerOptional.isPresent()) return Collections.emptyList();
            mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData(prototypeConnectionManagerOptional.get(), prototypeServer, (SQLCreateViewStatement) sqlStatement);
        }
        return CalciteConvertors.getColumnInfo(Objects.requireNonNull(mycatRowMetaData));
    }


    public Map<String, NormalTableConfig> getDefaultNormalTable(String targetName, String schemaName, Predicate<String> tableFilter) {
        Set<String> tables = new HashSet<>();
        Optional<JdbcConnectionManager> jdbcConnectionManagerOptional = getPrototypeConnectionManager();
        if (!jdbcConnectionManagerOptional.isPresent()) {
            return Collections.emptyMap();
        }
        try (DefaultConnection connection = jdbcConnectionManagerOptional.get().getConnection(targetName)) {
            RowBaseIterator tableIterator = connection.executeQuery("show tables from " + schemaName);
            while (tableIterator.next()) {
                tables.add(tableIterator.getString(0));
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            return Collections.emptyMap();
        }
        Map<String, NormalTableConfig> res = new ConcurrentHashMap<>();
        tables.stream().filter(tableFilter).parallel().forEach(tableName -> {
            NormalBackEndTableInfoConfig normalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig(targetName, schemaName, tableName);
            try {
                String createTableSQLByJDBC = getCreateTableSQLByJDBC(schemaName, tableName,
                        Collections.singletonList(new BackendTableInfo(targetName, schemaName, tableName))).orElse(null);
                if (createTableSQLByJDBC != null) {
                    res.put(tableName, new NormalTableConfig(createTableSQLByJDBC, normalBackEndTableInfoConfig));
                } else {
                    //exception
                }
            } catch (Throwable e) {
                LOGGER.warn("", e);
            }
        });
        return res;
    }

}
