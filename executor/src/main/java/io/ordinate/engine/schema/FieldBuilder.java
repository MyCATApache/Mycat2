/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.schema;

import lombok.Getter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Collections;

@Getter
public class FieldBuilder {
    final String name;
    final ArrowType type;
    final boolean nullable;

    public static FieldBuilder of(String name, ArrowType type, boolean nullable) {
        return new FieldBuilder(name, type, nullable);
    }

    public FieldBuilder(String name, ArrowType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public org.apache.arrow.vector.types.pojo.Field toArrow() {
        FieldType fieldType = new FieldType(nullable, type, null);
        return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, Collections.emptyList());
    }

}
