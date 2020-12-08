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

import io.mycat.MetaClusterCurrent;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import io.mycat.util.NameMap;
import lombok.Data;

import java.util.Map;

@Data
public class DrdsConfig implements DrdsConst {


    @Override
    public NameMap<SchemaHandler> schemas() {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        NameMap< SchemaHandler> schemaMap = metadataManager.getSchemaMap();
        return schemaMap;
    }

//    @Override
//    public MycatTableFactory getMycatTableFactory() {
//        return new MycatTableFactory() {
//            @Override
//            public AbstractMycatTable create(String schemaName, String createTableSql, DrdsConst drdsConst) {
//                return new MycatTableAdapter(schemaName,createTableSql,drdsConst);
//            }
//        };
//    }
}