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

import com.joanzapata.utils.Strings;
import io.mycat.beans.mycat.TransactionType;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class Context {
    private String name;
    private final String sql;
    private final java.util.Map<String, Collection<String>> tables;
    private final Map<String, String> names;
    private final Map<String, String> tags;
    private final String command;
    private final String explain;

    public Context(String name,String sql, Map<String, Collection<String>> tables, Map<String, String> names, Map<String, String> tags, String type, String explain) {
        this.name = name;
        this.sql = sql;
        this.tables = tables;
        this.names = names;
        this.tags = tags;
        this.command = type;
        this.explain = explain;
    }

    public String getExplain() {
        if (explain == null) {
            return sql;
        } else {
            Strings.Builder format = Strings.format(explain, "{", "}").strictMode(false);
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
        String s = names.get(name);
        if (s != null) {
            return s;
        }
        String s1 = tags.get(name);
        if (s1 != null) {
            return s1;
        }
        return defaultName;
    }

    public String getCommand() {
        return command;
    }

    public String getName() {
        return name;
    }
}
