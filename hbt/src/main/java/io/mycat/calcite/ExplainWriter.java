/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.calcite;

import lombok.Data;

@Data
public class ExplainWriter {
    private int column = 0;
    private StringBuilder text = new StringBuilder();
    private StringBuilder stringBuilder = new StringBuilder();
    public ExplainWriter item(String key, Object value,boolean condition){
        if (condition){
            return item(key, value);
        }
        return this;
    }
    public ExplainWriter item(String key, Object value) {
        stringBuilder.append("\n");
        for (int i = 0; i < column; i++) {
            stringBuilder.append(" ");
        }
        stringBuilder.append('\"').append(key).append('\"').append(" : ");
        if (value instanceof String) {
            stringBuilder.append('\"').append(value).append('\"');
        } else {
            stringBuilder.append(value);
        }
        return this;
    }

    public ExplainWriter name(String name) {
        stringBuilder.append("\n");
        for (int i = 0; i < column; i++) {
            stringBuilder.append(" ");
        }
        stringBuilder.append(name).append("(");
        column++;
        return this;
    }

    public ExplainWriter ret() {
        stringBuilder.append("\n");
        for (int i = 0; i < column; i++) {
            stringBuilder.append(" ");
        }
        column--;
        text.append(stringBuilder.toString());
        stringBuilder.setLength(0);
        return this;
    }

    public ExplainWriter into() {
        for (int i = 0; i < column; i++) {
            stringBuilder.append(" ");
        }
        String s = stringBuilder.toString().trim();
        stringBuilder.setLength(0);
        if (s.endsWith(",")){
            s = s.substring(0,s.length()-1);
        }
        s = s+")";
        text.append("\n").append(s);
        return this;
    }
}