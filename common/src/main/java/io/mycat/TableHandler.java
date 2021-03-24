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


import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface TableHandler {

    public LogicTableType getType();

    String getSchemaName();

    String getTableName();

    String getCreateTableSQL();

    default List<SimpleColumnInfo> getPrimaryKeyList(){
        return getColumns().stream()
                .filter(SimpleColumnInfo::isPrimaryKey)
                .collect(Collectors.toList());
    }

    List<SimpleColumnInfo> getColumns();

    Map<String,IndexInfo> getIndexes();

    SimpleColumnInfo getColumnByName(String name);

    SimpleColumnInfo getAutoIncrementColumn();

    String getUniqueName();

    Supplier<Number> nextSequence();

    default boolean isAutoIncrement() {
        return getAutoIncrementColumn() != null;
    }

    void createPhysicalTables();

    void dropPhysicalTables();


}