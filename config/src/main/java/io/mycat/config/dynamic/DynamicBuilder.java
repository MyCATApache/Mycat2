package io.mycat.config.dynamic;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

public class DynamicBuilder {

    final List<TextItem> petterns = new ArrayList<>();
    final List<SchemaItem> schemaPetterns = new ArrayList<>();

    public static void main(String[] args) {
        DynamicBuilder builder = new DynamicBuilder();
        builder.add("select * from travelrecord;", "$proxy($SQL,\"dataNode1\")");
        builder.add("select * from {db} travelrecord;", "$proxy($removeSchema($db,$SQL),\"dataNode1\")");

        builder.add("set XA = 1;", " setXA(true)");
        builder.add("set XA = 0;", "setXA(false)");

        builder.add("begin;", "!$XA?$proxyOnMaster(\"begin;\".\"dataNode1\"):$JDBC.begin()");
        builder.add("commit;", "!$XA?$proxyOnMaster(\"commit;\".\"dataNode1\"):$JDBC.commit()");

        builder.addSchema("TESTDB.travelrecord", "select {}", "return $proxyOnBalance($autoRemoveSchema($SQL),\"dataNode1\")");
        builder.addSchema("TESTDB.travelrecord", "{}", "return $proxyOnMaster($autoRemoveSchema($SQL),\"dataNode1\")");
        builder.addSchema("TESTDB.travelrecord,TESTDB.user", "select {}", "return $calcite($SQL)");

    }

    private void addSchema(String schema, String pattern, String code) {
        String[] split = schema.split(",");
        Set<SchemaTable> set = Arrays.stream(split).map(s -> s.split("."))
                .map(split1 -> new SchemaTable(split1[0].intern(), split1[1].intern())).collect(Collectors.toSet());
        schemaPetterns.add(new SchemaItem(set, pattern, code));
    }

    private void add(String pettern, String code) {
        petterns.add(new TextItem(pettern, code));
    }

    private HashMap<Integer, List<Item>> build(PatternComplier complier) {
        Map<Integer, Item> textItems = new HashMap<>();
        for (TextItem pettern : petterns) {
            int id = complier.complie(pettern.pettern);
            if (textItems.put(id, pettern) != null) {
                throw new UnsupportedOperationException();
            }
        }

        Map<Integer, List<SchemaItem>> delayDecisionSet = new HashMap<>();
        for (SchemaItem schemaPettern : schemaPetterns) {
            int id = complier.complie(schemaPettern.pettern);
            if (!textItems.containsKey(id)) {
                List<SchemaItem> list = delayDecisionSet.computeIfAbsent(id, integer -> new ArrayList<>());
                list.add(schemaPettern);
            } else {
                throw new UnsupportedOperationException();
            }
        }
        HashSet<Set<SchemaTable>> set = new HashSet<>();
        for (Map.Entry<Integer, List<SchemaItem>> entry : delayDecisionSet.entrySet()) {
            for (SchemaItem schemaItem : entry.getValue()) {
                if (!set.add(schemaItem.getSchemas())) {
                    throw new UnsupportedOperationException();
                }
            }
        }

        HashMap<Integer, List<Item>> matcher = new HashMap<>();
        textItems.forEach((key, value) -> matcher.put(key, Collections.singletonList(value)));
        for (Map.Entry<Integer, List<SchemaItem>> integerListEntry : delayDecisionSet.entrySet()) {
            List<Item> items = matcher.computeIfAbsent(integerListEntry.getKey(), integer -> new ArrayList<>());
            items.addAll(integerListEntry.getValue());
        }
        return matcher;
    }


    public interface PatternComplier {
        int complie(String pettern);
    }

    public interface Item {
    }

    @AllArgsConstructor
    private static class TextItem implements Item {
        String pettern;
        String code;
    }

    @AllArgsConstructor
    @Getter
    private static class SchemaItem implements Item {
        Set<SchemaTable> schemas;
        String pettern;
        String code;
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    private static class SchemaTable {
        String schemaName;
        String tableName;
    }
}