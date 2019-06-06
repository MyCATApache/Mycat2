/**
 * Copyright (C) <2019>  <gaozhiwen>
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
package io.mycat.config.schema;

import io.mycat.config.Configurable;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 对应schema.yml文件
 *
 * date: 10/09/2017
 * @author: gaozhiwen
 */
public class SchemaRootConfig implements Configurable {
    private List<SchemaConfig> schemas;

    private List<DataNodeConfig> dataNodes = new ArrayList<DataNodeConfig>();

    public List<SchemaConfig> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<SchemaConfig> schemas) {
        this.schemas = schemas;
    }

    public List<DataNodeConfig> getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(List<DataNodeConfig> dataNodes) {
        this.dataNodes = dataNodes;
    }

    public String defaultSchemaName;

    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    public void setDefaultSchemaName(String defaultSchemaName) {
        this.defaultSchemaName = defaultSchemaName;
    }
}
