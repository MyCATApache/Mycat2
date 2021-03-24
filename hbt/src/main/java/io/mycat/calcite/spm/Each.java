/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.calcite.spm;


import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@Builder
public class Each implements Comparable<Each> {
    String targetName;
    String sql;

    public Each(String targetName, String sql) {
        this.targetName = targetName;
        this.sql = sql.replaceAll("\n", " ").replaceAll("\r", " ");
    }

    public static Each of(String targetName, String sql) {
        return new Each(targetName, sql);
    }

    @Override
    public int compareTo(Each o) {
        int i = this.targetName.compareTo(o.targetName);
        if (i == 0) {
            return this.sql.compareTo(o.sql);
        }
        return i;
    }
}