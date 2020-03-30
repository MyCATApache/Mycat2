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

package io.mycat.client;

import io.mycat.hint.GlobalSequenceHint;
import io.mycat.hint.Hint;
import io.mycat.hint.NatureValueHint;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Junwen Chen
 **/
@ToString
public class Context {
    public static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(Context.class);
    private String name;
    private final String sql;
    private final java.util.Map<String, Collection<String>> tables;
    private final Map<String, String> names;
    private final Map<String, String> tags;
    private List<String> hints;
    private final String command;
    private final String explain;
    private Integer sqlId;
    private boolean cache = false;
    private Boolean simply;
    private static final ConcurrentHashMap<String, Hint> HINTS = new ConcurrentHashMap<>();
    //cache
    private String res;

    static {
        HINTS.put(NatureValueHint.INSTANCE.getName(), NatureValueHint.INSTANCE);
        HINTS.put(GlobalSequenceHint.INSTANCE.getName(), GlobalSequenceHint.INSTANCE);
    }

    public Context(String name,
                   String sql,
                   Map<String, Collection<String>> tables,
                   Map<String, String> names,
                   Map<String, String> tags,
                   List<String> hints,
                   String type,
                   String explain,
                   Integer sqlId,
                   boolean cache, Boolean simply) {
        this.name = name;
        this.sql = sql;
        this.tables = tables;
        this.names = Objects.requireNonNull(names);
        this.tags = tags;
        this.hints = hints;
        this.command = type;
        this.explain = explain;
        this.sqlId = sqlId;
        this.cache = cache;
        this.simply = simply;
    }

    public String getExplain() {
        if (res == null) {
            return res = innerExplain();
        } else {
            return res;
        }
    }

    private String innerExplain() {
        if (this.hints != null) {
            for (String hint : this.hints) {
                LOGGER.debug("hint:{}", hint);
                Hint hint1 = HINTS.get(hint);
                hint1.accept(this);
            }
        }
        if (explain == null) {
            return sql;
        } else {
            Builder format = new Builder(explain);
            for (Map.Entry<String, String> entry : names.entrySet()) {
                format.with(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                format.with(entry.getKey(), entry.getValue());
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


    public String getVariable(String name) {
        return getVariable(name, null);
    }

    public String getVariable(String name, String defaultName) {
        String s2 = getInnerVariable(name);
        if (s2 != null) {
            if (s2.startsWith("$")) {
                return getVariable(s2.substring(1), defaultName);
            } else {
                return s2;
            }
        }
        return defaultName;
    }

    @Nullable
    private String getInnerVariable(String name) {
        String s = names.get(name);
        if (s != null) {
            return s;
        }
        String s1 = tags.get(name);
        if (s1 != null) {
            return s1;
        }
        return null;
    }

    public String getCommand() {
        return command;
    }

    public String getName() {
        return name;
    }

    public void putVaribale(String name, String var) {
        this.names.put(name, var);
    }

    public static class Builder {
        private String baseString;


        private Builder(String string) {
            baseString = string;
        }

        public Builder with(String key, Object value) {
            if (value == null) value = "";
            baseString = baseString.replace("{" + key + "}", value.toString());
            return this;
        }

        public String build() {
            return baseString;
        }
    }

    public Integer getSqlId() {
        return sqlId;
    }

    public boolean isCache() {
        return cache;
    }

    public boolean isSimply() {
        return simply == Boolean.TRUE;
    }

    public SchemaTableObject getTableForUpdateOpt(){
        Map<String, Collection<String>> tables = this.getTables();
        if (tables.size() != 1) {
            throw new UnsupportedOperationException();
        }
        Map.Entry<String, Collection<String>> next = tables.entrySet().iterator().next();
        if (next.getValue().size() != 1) {
            throw new UnsupportedOperationException();
        }
        String schemaName = next.getKey();
        String tableName = next.getValue().iterator().next();
        return new SchemaTableObject(schemaName,tableName);
    }

    @AllArgsConstructor
    @Data
    public static class SchemaTableObject{
        String schema;
        String table;
    }
}
