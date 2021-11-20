/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Optional;

/**
 * chenjunwen
 */

public class ShowTableStatusSQLHandler extends AbstractSQLHandler<MySqlShowTableStatusStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowTableStatusSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowTableStatusStatement> request, MycatDataContext dataContext, Response response) {

        MySqlShowTableStatusStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
        SQLName database = ast.getDatabase();

        if (database == null) {
            return response.sendError(new MycatException("NO DATABASES SELECTED"));
        }
        String sql = toNormalSQL(request.getAst());
        return response.sendResultSet(runAsRowIterator(dataContext, sql));
    }

    private String toNormalSQL(MySqlShowTableStatusStatement ast) {
        SQLName database = ast.getDatabase();
        SQLName tableGroup = ast.getTableGroup();
        SQLExpr like = ast.getLike();
        SQLExpr where = ast.getWhere();

        return generateSimpleSQL(Arrays.asList(
                        new String[]{"TABLE_NAME","Name"},
                        new String[]{"ENGINE","Engine"},
                        new String[]{"VERSION","Version"},
                        new String[]{"ROW_FORMAT","Row_format"},
                        new String[]{"TABLE_ROWS","Rows"},
                        new String[]{"AVG_ROW_LENGTH","Avg_row_length"},
                        new String[]{"DATA_LENGTH","Data_length"},
                        new String[]{"MAX_DATA_LENGTH","Max_data_length"},
                        new String[]{"INDEX_LENGTH","Index_length"},
                        new String[]{"DATA_FREE","Data_free"},
                        new String[]{"AUTo_INCREMENT","Auto_increment"},
                        new String[]{"CREATE_TIME","Create_time"},
                        new String[]{"UPDATE_TIME","Update_time"},
                        new String[]{"CHECK_TIME","Check_time"},
                        new String[]{"TABLE_COLLATION","Collation"},
                        new String[]{"CHECKSUM","Checksum"},
                        new String[]{"CREATE_OPTIONS","Create_options"},
                        new String[]{"TABLE_COMMENT","Comment"}),
                "INFORMATION_SCHEMA", "TABLES", SQLUtils.toSQLExpr(" TABLES.TABLE_SCHEMA = '" + database.getSimpleName()+"'").toString(),
                Optional.ofNullable( where).map(i->i.toString()).orElse(null),
                Optional.ofNullable( like).map(i->"TABLE_NAME like"+i).orElse(null)).toString();
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
