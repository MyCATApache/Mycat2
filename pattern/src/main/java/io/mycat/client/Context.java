package io.mycat.client;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collection;
import java.util.Map;

@AllArgsConstructor
@Getter
public class Context {
    final String sql;
    final java.util.Map<String, Collection<String>> tables;
    final Map<String, String> names;
    final java.util.Map<String, String> tags;
    final String type;
    final String explain;

    public String getVariable(String name) {
        String s = names.get(name);
        if (s == null) {
            return tags.get(name);
        }
        return s;
    }
}
