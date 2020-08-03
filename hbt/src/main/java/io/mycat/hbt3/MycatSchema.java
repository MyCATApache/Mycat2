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
package io.mycat.hbt3;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class MycatSchema extends AbstractSchema {
    private List<String> createTableSqls = new ArrayList<>();
    private String schemaName;
    private DrdsConst drdsConst;
    private transient ImmutableMap<String, AbstractMycatTable> mycatTableMap = ImmutableMap.of();

    public void init() {
        ImmutableMap.Builder<String, AbstractMycatTable> builder = ImmutableMap.builder();
        MycatTableFactory tableFactory = drdsConst.getMycatTableFactory();
        for (String sql : createTableSqls) {
            AbstractMycatTable table = tableFactory.create(this.schemaName, sql,drdsConst);
            builder.put(table.getTableName(), table);
        }
        this.mycatTableMap = builder.build();
    }

    @SneakyThrows
    @Override
    protected Map<String, Table> getTableMap() {
        return (Map) mycatTableMap;
    }
}
