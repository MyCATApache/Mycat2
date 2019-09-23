/**
 * Copyright (C) <2019>  <chen junwen>
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
package cn.lightfish.pattern;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.*;


/**
 * db1.table1.column1
 * db1.table1
 * table1.column1
 * column1
 * table1
 * <p>
 * dafault
 * <p>
 * column1 ->dafault.column1
 * table1->dafault.table1
 * <p>
 * <p>
 * db1.table1->
 * table1.column1
 */

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public final class TableCollector implements GPatternTokenCollector {
    private final int dotHash;
    private final IntObjectHashMap<TableInfo> map;
    private final TableInfo[] tables = new TableInfo[32];
    private final TableCollectorBuilder builder;
    private int tableIndex;
    private int state = State.EXCPECT_ID;
    private int currentSchemaHash;
    private int first;
    private int second;

    public TableCollector(TableCollectorBuilder builder) {
        this.builder = builder;
        this.dotHash = builder.dotHash;
        this.map = builder.map;
    }

    public void useSchema(String schema) {
        Integer intHash = builder.schemaHash.get(schema);
        if (intHash == null) throw new UnsupportedOperationException();
        currentSchemaHash = intHash;
    }


    @Override
    public void onCollectStart() {
        this.tableIndex = 0;
    }

    @Override
    public void collect(GPatternSeq token) {
        int hash = token.hashCode();
        switch (state) {
            case State.EXCPECT_ID: {
                if (hash == dotHash) {
                    state = State.EXPECT_ATTRIBUTE;
                } else {
                    first = hash;
                    collect(first);
                }
                break;
            }
            case State.EXPECT_ATTRIBUTE: {
                second = hash;
                collect(first, second);
                clearState();
                state = State.EXPECT_DOT;
                break;
            }
            case State.EXPECT_DOT: {
                if (hash == dotHash) {
                    state = State.EXPECT_TABLE;
                } else {
                    state = State.EXCPECT_ID;
                }
                break;
            }
            default: {//State.EXPECT_TABLE
                collect(first, second);
                clearState();
                state = State.EXCPECT_ID;
                break;
            }
        }
    }

    @Override
    public void onCollectEnd() {
    }

    private void collect(int first) {
        int hash = currentSchemaHash ^ first;
        add(hash);
    }

    private void add(int hash) {
        TableInfo o = map.get(hash);
        if (o != null) {
            tables[tableIndex++] = o;
        }
    }

    private void collect(int first, int second) {
        int hash = first;
        hash = hash ^ second;
        add(hash);
    }

    private void clearState() {
//        this.first = 0;
//        this.second = 0;
    }

    public TableInfo[] getTableArray() {
        return Arrays.copyOf(tables, tableIndex);
    }

    public Map<String, Collection<String>> geTableMap(Map<String, Collection<String>> map) {
        for (int i = 0; i < this.tableIndex; i++) {
            TableInfo table = tables[i];
            map.computeIfAbsent(table.getSchemaName(), s -> new HashSet<>()).add(table.getTableName());
        }
        return map;
    }

    public Map<String, Collection<String>> geTableMap() {
        return geTableMap(new HashMap<>());
    }

    public boolean isMatch() {
        return tableIndex > 0;
    }


    interface State {
        int EXCPECT_ID = 0;
        int EXPECT_ATTRIBUTE = 1;
        int EXPECT_DOT = 2;
        int EXPECT_TABLE = 3;
    }


    @AllArgsConstructor
    @Getter
    static class TableInfo {
        final String schemaName;
        final String tableName;
        final int schema;
        final int table;
        final int hash;
    }


}