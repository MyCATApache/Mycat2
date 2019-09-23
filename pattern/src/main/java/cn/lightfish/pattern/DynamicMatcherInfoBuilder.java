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

import java.util.*;

/**
 * https://github.com/junwen12221/GPattern.git
 * @author Junwen Chen
 **/
public class DynamicMatcherInfoBuilder {

    final ArrayList<TextItem> petterns = new ArrayList<>();
    final ArrayList<SchemaItem> schemaPetterns = new ArrayList<>();
    final HashMap<String, Collection<String>> tableMap = new HashMap<>();
    final HashMap<Set<SchemaTable>, SchemaItem> tableInstructionMap = new HashMap<>();
    final HashMap<Integer, List<Item>> ruleInstructionMap = new HashMap<>();

    private void addTable(String schemaName, String tableName) {
        Collection<String> set = tableMap.computeIfAbsent(schemaName.toUpperCase(), (s) -> new HashSet<>());
        set.add(tableName.toUpperCase());
    }

    public void addSchema(String schema, String pattern, String code) {
        String[] split = schema.split(",");
        Set<SchemaTable> set = new HashSet<>();
        for (String s : split) {
            String[] split1 = s.split("\\.");
            String schemaName = split1[0].intern();
            String tableName = split1[1].intern();
            addTable(schemaName, tableName);
            SchemaTable schemaTable = new SchemaTable(schemaName, tableName);
            set.add(schemaTable);
        }
        schemaPetterns.add(new SchemaItem(set, pattern, code));
    }

    public void add(String pettern, String code) {
        petterns.add(new TextItem(pettern, code));
    }

    public void build(PatternComplier complier) {
        Map<Integer, TextItem> textItems = new HashMap<>();
        for (TextItem pettern : petterns) {
            int id = complier.complie(pettern.getPettern());
            TextItem tmp;
            if ((tmp = textItems.put(id, pettern)) != null) {
                throw new GPatternException.PatternConflictException("{0} and {1} is conflict", tmp, pettern);
            }
        }

        Map<Integer, List<SchemaItem>> delayDecisionSet = new HashMap<>();
        for (SchemaItem schemaPettern : schemaPetterns) {
            if (schemaPettern.getPettern() != null) {
                int id = complier.complie(schemaPettern.getPettern());
                if (!textItems.containsKey(id)) {
                    List<SchemaItem> list = delayDecisionSet.computeIfAbsent(id, integer -> new ArrayList<>());
                    list.add(schemaPettern);
                } else {
                    throw new GPatternException.PatternConflictException("{0} and {1} is conflict", textItems.get(id), schemaPettern);
                }
            } else {
                if (!tableInstructionMap.containsKey(schemaPettern.getSchemas())) {
                    tableInstructionMap.put(schemaPettern.getSchemas(), schemaPettern);
                } else {
                    throw new GPatternException.PatternConflictException("{0} and {1} is conflict", tableInstructionMap.get(schemaPettern.getSchemas()), schemaPettern);
                }
            }
        }
        HashSet<Set<SchemaTable>> set = new HashSet<>();
        for (Map.Entry<Integer, List<SchemaItem>> entry : delayDecisionSet.entrySet()) {
            for (SchemaItem schemaItem : entry.getValue()) {
                if (!set.add(schemaItem.getSchemas())) {
                    throw new GPatternException.PatternConflictException("{0} is conflict", schemaItem);
                }
            }
        }

        textItems.forEach((key, value) -> ruleInstructionMap.put(key, Collections.singletonList(value)));
        for (Map.Entry<Integer, List<SchemaItem>> integerListEntry : delayDecisionSet.entrySet()) {
            List<Item> items = ruleInstructionMap.computeIfAbsent(integerListEntry.getKey(), integer -> new ArrayList<>());
            items.addAll(integerListEntry.getValue());
        }
    }

    public Map<String, Collection<String>> getTableMap() {
        return tableMap;
    }

    public interface PatternComplier {
        int complie(String pettern);
    }

    public HashMap<Integer, List<Item>> getRuleInstructionMap() {
        return ruleInstructionMap;
    }
}