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
package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TableCollector {

    public static Map<String, Collection<String>> collect(String defaultSchema, String sql) {
        Map<String, Collection<String>> collectionMap = new HashMap<>();
        try {
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            sqlStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    String schema = x.getSchema();
                    String tableName = x.getTableName();
                    if (schema == null) {
                        schema = defaultSchema;
                    }
                    if (schema == null) {
                        throw new UnsupportedOperationException("please use schema");
                    }
                    schema = SQLUtils.normalize(schema);
                    tableName = SQLUtils.normalize(tableName);
                    Collection<String> strings = collectionMap.computeIfAbsent(schema, s -> new HashSet<>());
                    strings.add(tableName);
                    return super.visit(x);
                }
            });
        } catch (Throwable ignored) {

        }
        return collectionMap;
    }
}