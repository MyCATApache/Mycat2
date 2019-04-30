/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans;

import io.mycat.config.MycatConfig;
import io.mycat.config.schema.SchemaConfig;
import io.mycat.proxy.MycatRuntime;

public class Schema {
    final SchemaConfig schemaConfig;
    final DataNode defaultDataNode;

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public Schema(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        MycatConfig mycatConfig = MycatRuntime.INSTANCE.getMycatConfig();
        defaultDataNode = mycatConfig.getDataNodeMap().get(schemaConfig.getDefaultDataNode());
    }

    public DataNode getDefaultDataNode() {
        return defaultDataNode;
    }

    public String getSchemaName() {
        return schemaConfig.getName();
    }
}
