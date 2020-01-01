/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlEngine.ast.complier;

import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import io.mycat.sqlEngine.ast.SQLTypeMap;
import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.schema.DbSchemaManager;
import io.mycat.sqlEngine.schema.DbTable;
import io.mycat.sqlEngine.schema.TableColumnDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
/**
 * @author Junwen Chen
 **/
public class ColumnAllocator {

    private final ComplierContext complierContext;
    private final HashMap<SQLColumnDefinition, Integer> columnIndexMap;
    private final HashMap<SQLTableSource, List<SQLColumnDefinition>> tableSourceColumnMap;
    private final HashMap<SQLTableSource, Integer> tableSourceColumnStartIndexMap;

    public ColumnAllocator(ComplierContext complierContext,
                           HashMap<SQLColumnDefinition, Integer> columnIndexMap,
                           HashMap<SQLTableSource, List<SQLColumnDefinition>> tableSourceColumnMap,
                           HashMap<SQLTableSource, Integer> tableSourceColumnStartIndexMap) {
        this.complierContext = complierContext;
        this.columnIndexMap = columnIndexMap;
        this.tableSourceColumnMap = tableSourceColumnMap;
        this.tableSourceColumnStartIndexMap = tableSourceColumnStartIndexMap;
    }

    public <T extends Comparable<T>> ValueExpr<T> getFieldExecutor(
            SQLColumnDefinition resolvedColumn) {
        int index = columnIndexMap.getOrDefault(resolvedColumn, -1);
        Class type = SQLTypeMap.toClass(resolvedColumn.jdbcType());
        Object[] scope = complierContext.runtimeContext.scope;
        return new ValueExpr<T>() {
            @Override
            public Class<T> getType() {
                return type;
            }

            @Override
            public T getValue() {
                return (T) scope[index];
            }
        };
    }

    public TableColumnDefinition[] getLeafTableColumnDefinition(SQLExprTableSource tableSource) {
        SchemaObject tableObject = tableSource.getSchemaObject();
        DbTable table = DbSchemaManager.INSTANCE
                .getTable(tableObject.getSchema().getName(), tableObject.getName());
        List<SQLColumnDefinition> columnDefinitions = tableSourceColumnMap.get(tableSource);
        TableColumnDefinition[] mycatColumnDefinitions = new TableColumnDefinition[columnDefinitions
                .size()];
        for (int i = 0; i < mycatColumnDefinitions.length; i++) {
            SQLColumnDefinition columnDefinition = columnDefinitions.get(i);
            mycatColumnDefinitions[i] = table.getColumnByName(columnDefinition.getColumnName());
        }
        return mycatColumnDefinitions;
    }

    public int scopeSize() {
        return columnIndexMap.size();
    }

    public int getTableStartIndex(SQLExprTableSource tableSource) {
        return tableSourceColumnStartIndexMap.get(tableSource);
    }

    public List<SQLColumnDefinition> getColumnDefinitionBySQLTableSource(SQLTableSource sqlTableSource) {
        List<SQLColumnDefinition> sqlColumnDefinitions = tableSourceColumnMap.get(sqlTableSource);
        return sqlColumnDefinitions == null ? Collections.emptyList() : new ArrayList<>(sqlColumnDefinitions);
    }
}