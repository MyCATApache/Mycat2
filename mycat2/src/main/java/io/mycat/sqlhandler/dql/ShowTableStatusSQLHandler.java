package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Optional;

/**
 * chenjunwen
 */

public class ShowTableStatusSQLHandler extends AbstractSQLHandler<MySqlShowTableStatusStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowTableStatusSQLHandler.class);

    @Override
    protected void onExecute(SQLRequest<MySqlShowTableStatusStatement> request, MycatDataContext dataContext, Response response) throws Exception {

        MySqlShowTableStatusStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
        SQLName database = ast.getDatabase();
        if (database == null){
            response.sendError(new MycatException("NO DATABASES SELECTED"));
            return ;
        }
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        Optional<SchemaHandler> schemaHandler = Optional.ofNullable(metadataManager.getSchemaMap()).map(i -> i.get(SQLUtils.normalize(ast.getDatabase().toString())));
        String targetName = schemaHandler.map(i -> i.defaultTargetName()).map(name -> dataContext.resolveDatasourceTargetName(name)).orElse(null);
        if (targetName != null) {
            response.proxySelect(targetName, ast.toString());
        } else {
            response.tryBroadcastShow(ast.toString());
        }
        return ;
//        MySqlShowTableStatusStatement ast = request.getAst();
//        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
//            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
//        }
//        SQLName database = ast.getDatabase();
//        if (database == null){
//            response.sendError(new MycatException("NO DATABASES SELECTED"));
//            return ExecuteCode.PERFORMED;
//        }
//        Optional<SchemaHandler> schemaHandler = Optional.ofNullable(MetadataManager.INSTANCE.getSchemaMap()).map(i -> i.get(SQLUtils.normalize(ast.getDatabase().toString())));
//        String targetName = schemaHandler.map(i -> i.defaultTargetName()).map(name -> ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(name, true, null)).orElse(null);
//        if (targetName != null) {
//            response.proxySelect(targetName, ast.toString());
//        } else {
//            response.proxyShow(ast);
//        }

//        try {
//            DDLManager.INSTANCE.updateTables();
//            String databaseName = ast.getDatabase() == null ? dataContext.getDefaultSchema() :
//                    SQLUtils.normalize(ast.getDatabase().getSimpleName());
//
//            String tableName = ast.getTableGroup() == null ? null
//                    : SQLUtils.normalize(ast.getTableGroup().getSimpleName());
//
//            String sql = ShowStatementRewriter.showTableStatus(ast, databaseName, tableName);
//
//            try (RowBaseIterator query = MycatDBs.createClient(dataContext).query(sql)) {
//                response.sendResultSet(() -> query, () -> {
//                    throw new UnsupportedOperationException();
//                });
//            }
//        }catch (Exception e){
//            LOGGER.error("",e);
//            response.sendError(e);
//        }
//        return ExecuteCode.PERFORMED;
    }

    private void addColumns(ResultSetBuilder resultSetBuilder) {
        /**
         * MySQL Protocol
         *     Packet Length: 66
         *     Packet Number: 2
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Name
         *     Original name: TABLE_NAME
         *     Charset number: utf8mb4 COLLATE utf8mb4_unicode_ci (224)
         *     Length: 256
         *     Type: FIELD_TYPE_VAR_STRING (253)
         *     Flags: 0x0001
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Name", JDBCType.VARCHAR);

        /**
         * MySQL Protocol
         *     Packet Length: 64
         *     Packet Number: 3
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Engine
         *     Original name: ENGINE
         *     Charset number: utf8mb4 COLLATE utf8mb4_unicode_ci (224)
         *     Length: 256
         *     Type: FIELD_TYPE_VAR_STRING (253)
         *     Flags: 0x0000
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Engine", JDBCType.VARCHAR);

        /**
         * MySQL Protocol
         *     Packet Length: 66
         *     Packet Number: 4
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Version
         *     Original name: VERSION
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Version", JDBCType.BIGINT);


        /**
         * MySQL Protocol
         *     Packet Length: 72
         *     Packet Number: 5
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Row_format
         *     Original name: ROW_FORMAT
         *     Charset number: utf8mb4 COLLATE utf8mb4_unicode_ci (224)
         *     Length: 40
         *     Type: FIELD_TYPE_VAR_STRING (253)
         *     Flags: 0x0000
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Row_format", JDBCType.VARCHAR);

        /**
         * MySQL Protocol
         *     Packet Length: 66
         *     Packet Number: 6
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Rows
         *     Original name: TABLE_ROWS
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         */

        resultSetBuilder.addColumnInfo("Rows", JDBCType.BIGINT);

        /**
         * MySQL Protocol
         *     Packet Length: 80
         *     Packet Number: 7
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Avg_row_length
         *     Original name: AVG_ROW_LENGTH
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         *
         *
         */
        resultSetBuilder.addColumnInfo("Avg_row_length", JDBCType.BIGINT);

        /**
         *  MySQL Protocol
         *     Packet Length: 74
         *     Packet Number: 8
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Data_length
         *     Original name: DATA_LENGTH
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Data_length", JDBCType.BIGINT);

        /**
         * MySQL Protocol
         *     Packet Length: 82
         *     Packet Number: 9
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Max_data_length
         *     Original name: MAX_DATA_LENGTH
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Max_data_length", JDBCType.BIGINT);

        /**
         * MySQL Protocol
         *     Packet Length: 76
         *     Packet Number: 10
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Index_length
         *     Original name: INDEX_LENGTH
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Index_length", JDBCType.BIGINT);

        /**
         * MySQL Protocol
         *     Packet Length: 70
         *     Packet Number: 11
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Data_free
         *     Original name: DATA_FREE
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Data_free", JDBCType.BIGINT);

        /**
         * MySQL Protocol
         *     Packet Length: 80
         *     Packet Number: 12
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Auto_increment
         *     Original name: AUTO_INCREMENT
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Auto_increment", JDBCType.BIGINT);

        /**
         * MySQL Protocol
         *     Packet Length: 74
         *     Packet Number: 13
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Create_time
         *     Original name: CREATE_TIME
         *     Charset number: binary COLLATE binary (63)
         *     Length: 19
         *     Type: FIELD_TYPE_DATETIME (12)
         *     Flags: 0x0080
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Create_time", JDBCType.TIME);

        /**
         * MySQL Protocol
         *     Packet Length: 74
         *     Packet Number: 14
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Update_time
         *     Original name: UPDATE_TIME
         *     Charset number: binary COLLATE binary (63)
         *     Length: 19
         *     Type: FIELD_TYPE_DATETIME (12)
         *     Flags: 0x0080
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Update_time", JDBCType.TIME);

        /**
         * MySQL Protocol
         *     Packet Length: 72
         *     Packet Number: 15
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Check_time
         *     Original name: CHECK_TIME
         *     Charset number: binary COLLATE binary (63)
         *     Length: 19
         *     Type: FIELD_TYPE_DATETIME (12)
         *     Flags: 0x0080
         *     Decimals: 0
         *
         *
         */
        resultSetBuilder.addColumnInfo("Check_time", JDBCType.TIME);

        /**
         * MySQL Protocol
         *     Packet Length: 76
         *     Packet Number: 16
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Collation
         *     Original name: TABLE_COLLATION
         *     Charset number: utf8mb4 COLLATE utf8mb4_unicode_ci (224)
         *     Length: 128
         *     Type: FIELD_TYPE_VAR_STRING (253)
         *     Flags: 0x0000
         *     Decimals: 0
         *
         *
         */
        resultSetBuilder.addColumnInfo("Collation", JDBCType.VARCHAR);

        /**
         * MySQL Protocol
         *     Packet Length: 68
         *     Packet Number: 17
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Checksum
         *     Original name: CHECKSUM
         *     Charset number: binary COLLATE binary (63)
         *     Length: 21
         *     Type: FIELD_TYPE_LONGLONG (8)
         *     Flags: 0x0020
         *     Decimals: 0
         *
         */
        resultSetBuilder.addColumnInfo("Checksum", JDBCType.BIGINT);

        /**
         * MySQL Protocol
         *     Packet Length: 80
         *     Packet Number: 18
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Create_options
         *     Original name: CREATE_OPTIONS
         *     Charset number: utf8mb4 COLLATE utf8mb4_unicode_ci (224)
         *     Length: 8192
         *     Type: FIELD_TYPE_VAR_STRING (253)
         *     Flags: 0x0000
         *     Decimals: 0
         *
         */
        resultSetBuilder.addColumnInfo("Create_options", JDBCType.VARCHAR);

        /**
         * MySQL Protocol
         *     Packet Length: 72
         *     Packet Number: 19
         *     Catalog: def
         *     Database: information_schema
         *     Table: TABLES
         *     Original table: TABLES
         *     Name: Comment
         *     Original name: TABLE_COMMENT
         *     Charset number: utf8mb4 COLLATE utf8mb4_unicode_ci (224)
         *     Length: 8192
         *     Type: FIELD_TYPE_VAR_STRING (253)
         *     Flags: 0x0001
         *     Decimals: 0
         */
        resultSetBuilder.addColumnInfo("Comment", JDBCType.VARCHAR);
    }
}
