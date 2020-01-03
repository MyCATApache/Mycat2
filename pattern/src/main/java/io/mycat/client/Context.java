package io.mycat.client;

import com.joanzapata.utils.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collection;
import java.util.Map;

@AllArgsConstructor
public class Context {
    private final String sql;
    private  final java.util.Map<String, Collection<String>> tables;
    private  final Map<String, String> names;
    private final Map<String, String> tags;
    private  final String type;
    private  final String explain;
    private  final String transactionType;

    public String getCommand() {
        if (explain == null){
            return sql;
        }else {
            Strings.Builder format = Strings.format(explain, "{", "}").strictMode(true);
            for (Map.Entry<String, String> entry : names.entrySet()) {
                format .with(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                format .with(entry.getKey(), entry.getValue());
            }
            return format.build();
        }
    }

    public Map<String, Collection<String>> getTables() {
        return tables;
    }

    public Map<String, String> getNames() {
        return names;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String getType() {
        return type;
    }

    public String getExplain() {
        return explain;
    }

    public String getTransactionType() {
        return transactionType;
    }



    public String getVariable(String name) {
        String s = names.get(name);
        if (s == null) {
            return tags.get(name);
        }
        return s;
    }
}
