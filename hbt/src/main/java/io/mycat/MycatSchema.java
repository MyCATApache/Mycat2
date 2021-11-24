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
package io.mycat;

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

@Data
public class MycatSchema extends AbstractSchema {
    private final String schemaName;
    private final Map<String, Table> mycatTableMap;

    public MycatSchema(String schemaName,
                       Map<String, Table> mycatTableMap) {
        this.schemaName = schemaName;
        this.mycatTableMap = mycatTableMap;
    }

    public  static MycatSchema create(String schemaName,
                                      Map<String, Table> mycatTableMap){
        return new MycatSchema(schemaName,mycatTableMap);
    }

    @SneakyThrows
    @Override
    protected Map<String, Table> getTableMap() {
        return (Map) mycatTableMap;
    }

}
