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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


@EqualsAndHashCode
@Builder
public class SpecificSql implements Comparable<SpecificSql> {
    String relNode;
    String parameterizedSql;
    List<Each> sqls;

    public SpecificSql(String relNode, String parameterizedSql, List<Each> sqls) {
        this.relNode = relNode.replaceAll("\n", " ").replaceAll("\r"," ");
        this.parameterizedSql = parameterizedSql.replaceAll("\n", " ").replaceAll("\r"," ");
        this.sqls = sqls;
    }
    public static SpecificSql of (String relNode, String parameterizedSql, Each... sqls) {
        return new SpecificSql(relNode, parameterizedSql, Arrays.asList(sqls));
    }

    @Override
    public int compareTo(SpecificSql o) {
        return this.parameterizedSql.compareTo(o.parameterizedSql);
    }

    @Override
    public String toString() {
        return sqls.stream().map(i->i.toString()).collect(Collectors.joining("\n"));
    }
}