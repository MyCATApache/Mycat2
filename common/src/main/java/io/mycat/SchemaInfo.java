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

import lombok.*;

import java.util.Objects;


@EqualsAndHashCode
@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
public class SchemaInfo {
    final String targetSchema;//todo 没有配置库名怎么处理
    final String targetTable;
    final String targetSchemaTable;

    public SchemaInfo(String targetSchema, String targetTable) {
        this.targetSchema = targetSchema;
        this.targetTable = Objects.requireNonNull(targetTable);

        if (this.targetSchema != null) {
            this.targetSchemaTable = this.targetSchema + "." + this.targetTable;
        } else {
            this.targetSchemaTable = this.targetTable;
        }
    }

}