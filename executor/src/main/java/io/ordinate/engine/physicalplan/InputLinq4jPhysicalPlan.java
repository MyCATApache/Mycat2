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

package io.ordinate.engine.physicalplan;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputLinq4jPhysicalPlan extends ValuesPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(InputLinq4jPhysicalPlan.class);

    public InputLinq4jPhysicalPlan(Schema schema, Iterable<Object[]> rowList) {
        super(schema, rowList);
    }



    public static InputLinq4jPhysicalPlan create(Schema schema, Iterable<Object[]> rowList) {
        return new InputLinq4jPhysicalPlan(schema,rowList);
    }
}
