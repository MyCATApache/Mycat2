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

package io.ordinate.engine.vector;

import io.ordinate.engine.schema.InnerType;
import io.questdb.cairo.map.MapValue;
import org.apache.arrow.vector.FieldVector;

public interface AggregateVectorExpression  {

    public default long computeFinalLongValue(MapValue resultValue){ return 0;}
    public default double computeFinalDoubleValue(MapValue resultValue){ return 0;}
    public void computeUpdateValue(MapValue resultValue, FieldVector input);

    public InnerType getType();

    public int getInputColumnIndex();
}
