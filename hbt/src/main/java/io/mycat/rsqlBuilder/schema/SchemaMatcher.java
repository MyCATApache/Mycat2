package io.mycat.rsqlBuilder.schema;

import io.mycat.describer.ParseNode;
import io.mycat.describer.literal.IdLiteral;

import java.util.HashMap;
import java.util.Map;

public class SchemaMatcher {

    final Map<String, Map<String, Map<String, ParseNode>>> map = new HashMap<>();

    public void addSchema(String schema, String table, String column) {
        Map<String, Map<String, ParseNode>> stringHashMapMap1 = map.computeIfAbsent(schema.toLowerCase(), s -> new HashMap<>());
        if (table != null) {
            Map<String, ParseNode> map = stringHashMapMap1.computeIfAbsent(table.toLowerCase(), (s) -> new HashMap<>());
            if (column != null) {
                map.put(column.toLowerCase(), null);
            }
        }

    }


    public ParseNode getSchemaObject(IdLiteral string) {
        String s = string.getId().toLowerCase();
        Map<String, Map<String, ParseNode>> stringMapMap = map.get(s);
        if (stringMapMap != null) {
            return new SchemaObject(s, stringMapMap);
        }
        return string.copy();
    }
}