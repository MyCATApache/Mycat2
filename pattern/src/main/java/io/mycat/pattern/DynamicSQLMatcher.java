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
package io.mycat.pattern;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public final class DynamicSQLMatcher<T> {
    private final TableCollector tableCollector;
    private final GPattern gPattern;
    private final HashMap<Integer, List<Item>> rule_tables_map;
    private final HashMap<Set<SchemaTable>, SchemaItem> table_map;

    public DynamicSQLMatcher(TableCollector tableCollector, GPattern gPattern, HashMap<Integer, List<Item>> runtimeMap, HashMap<Set<SchemaTable>, SchemaItem> table_map) {
        this.tableCollector = tableCollector;
        this.gPattern = gPattern;
        this.rule_tables_map = runtimeMap;
        this.table_map = table_map;
    }

    public T match(String sql) {
        return match(StandardCharsets.UTF_8.encode(sql));
    }

    public T match(ByteBuffer sql) {
        return getInstruction(gPattern.matcherAndCollect(sql), tableCollector);
    }

    private T getInstruction(GPatternMatcher matcher, TableCollector tableCollector) {
        boolean match = tableCollector.isMatch();
        if (matcher.acceptAll()) {
            int id = matcher.id();
            List<Item> items = rule_tables_map.get(id);
            if (items.size() == 1) {
                return (T) items.get(0).getInstruction();
            } else if (match) {
                Map<String, Collection<String>> collectionMap = tableCollector.geTableMap();
                for (Item item : items) {
                    SchemaItem schemaItem = (SchemaItem) item;
                    if (collectionMap.equals(schemaItem.getTableMap())) {
                        return (T) schemaItem.getInstruction();
                    }
                }
            } else {
                return null;
            }
        }

        if (match) {
            Map<String, Collection<String>> collectionMap = tableCollector.geTableMap();
            int hash = collectionMap.hashCode();
            SchemaItem schemaItem = table_map.get(hash);
            if (schemaItem == null) {
                return null;
            } else {
                return (T)schemaItem.getInstruction();
            }
        }
        return null;
    }

    public TableCollector getTableCollector() {
        return tableCollector;
    }

    public GPattern getgPattern() {
        return gPattern;
    }

    public HashMap<Integer, List<Item>> getRule_tables_map() {
        return rule_tables_map;
    }

    public HashMap<Set<SchemaTable>, SchemaItem>  getTable_map() {
        return table_map;
    }

    public GPatternUTF8Lexer getLexer() {
        return gPattern.getUtf8Lexer();
    }

    public Map<String, Collection<String>> geTables() {
        return getTableCollector().geTableMap();
    }

    public GPatternMatcher getResult() {
        return getgPattern().getMatcher();
    }

    public String group(String name) {
        return getNameAsString(name);
    }

    public String getNameAsString(String name) {
        return getResult().getName(name);
    }

    public String getSQLAsString() {
        GPatternUTF8Lexer lexer = getLexer();
        String string = lexer.getString(lexer.startOffset, lexer.limit);
        return string;
    }
}