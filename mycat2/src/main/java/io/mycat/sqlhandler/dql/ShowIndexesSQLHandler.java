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
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public class ShowIndexesSQLHandler extends AbstractSQLHandler<SQLShowIndexesStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowIndexesStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowIndexesStatement ast = request.getAst();

        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(dataContext.getDefaultSchema());
        }
        String sql = toNormalSQL(ast);
        List<ColumnDefPacket> showCreateFunctionColumns = PrototypeService.getShowIndexesColumns();
        return response.sendResultSet(runAsRowIterator(dataContext,sql));
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


    private String toNormalSQL(SQLShowIndexesStatement ast) {
        SQLExprTableSource table = ast.getTable();
        String schemaName = table.getSchema();
        String tableName = table.getName().getSimpleName();
        SQLExpr where = ast.getWhere();

        List<String> where0List = new ArrayList<>();
        String where0 = null;
        if (schemaName != null) {
            where0List.add(" information_schema.statistics.TABLE_SCHEMA =  '" + SQLUtils.normalize(schemaName) + "'");
        }
        if (tableName != null) {
            where0List.add(" information_schema.statistics.TABLE_NAME =  '" + SQLUtils.normalize(tableName) + "'");
        }
        if (where0List.isEmpty()) {
            where0 = null;
        } else {
            where0 = String.join(" and ", where0List);
        }

        return generateSimpleSQL(Arrays.asList(
                new String[]{"TABLE_NAME", "Table"},
                new String[]{"NON_UNIQUE", "Non_unique"},
                new String[]{"INDEX_NAME", "Key_name"},
                new String[]{"SEQ_IN_INDEX", "Seq_in_index"},
                new String[]{"COLUMN_NAME", "Column_name"},
                new String[]{"COLLATION", "Collation"},
                new String[]{"CARDINALITY", "Cardinality"},
                new String[]{"SUB_PART", "Sub_part"},
                new String[]{"PACKED", "Packed"},
                new String[]{"NULLABLE", "Null"},
                new String[]{"INDEX_TYPE", "Index_type"},
                new String[]{"COMMENT", "Comment"},
                new String[]{"INDEX_COMMENT", "Index_comment"}
        ), "information_schema", "statistics", where0, Optional.ofNullable(where).map(i -> i.toString()).orElse(null), null).toString();
    }
}
