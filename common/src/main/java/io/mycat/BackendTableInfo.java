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
package io.mycat;


import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
@Data
@EqualsAndHashCode
@ToString
@Builder
public class BackendTableInfo {
    private String targetName;
    private SchemaInfo schemaInfo;

    public BackendTableInfo(String targetName, SchemaInfo schemaInfo) {
        this.targetName = targetName;
        this.schemaInfo = schemaInfo;
    }

   public String getUniqueName(){
        return targetName+"."+schemaInfo.getTargetSchemaTable();
   }

}
