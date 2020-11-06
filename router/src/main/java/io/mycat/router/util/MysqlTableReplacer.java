/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.router.util;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.SchemaInfo;

import java.util.Map;
import java.util.Objects;

/**
 * 用于替代表名
 */
public class MysqlTableReplacer extends MySqlASTVisitorAdapter {
    private final Map<String, SchemaInfo> dbSet;
    private final String schemaName;

    public MysqlTableReplacer(Map<String, SchemaInfo> dbSet, String schemaName) {
        this.dbSet = dbSet;
        this.schemaName = schemaName;
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        String schemaName = x.getSchema();
        String tableName = x.getTableName();
        if (schemaName != null) {
            schemaName = SQLUtils.forcedNormalize(schemaName, DbType.mysql);
        }
        if (tableName != null) {
            tableName = SQLUtils.forcedNormalize(tableName, DbType.mysql);
        }
        if (schemaName == null) {
            schemaName = this.schemaName;
        }
        Objects.requireNonNull(tableName);
        SchemaInfo mappingTable = getMappingTable(schemaName, tableName);
        if (mappingTable!=null){
            x.setExpr(new SQLPropertyExpr(mappingTable.getTargetSchema(), mappingTable.getTargetTable()));
        }
        return super.visit(x);
    }


    public SchemaInfo getMappingTable(String schemaName, String tableName) {
        String key = schemaName + "." + tableName;
        return dbSet.get(key);
    }

}