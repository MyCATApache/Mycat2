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

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;


public class ArrowTypes {
    public static final ArrowType.Bool BOOLEAN_TYPE = new ArrowType.Bool();
    public static final ArrowType.Int INT8_TYPE = new ArrowType.Int(8, true);
    public static final ArrowType.Int INT16_TYPE = new ArrowType.Int(16, true);
    public static final ArrowType.Int INT32_TYPE = new ArrowType.Int(32, true);
    public static final ArrowType.Int INT64_TYPE = new ArrowType.Int(64, true);
    public static final ArrowType.Int UINT8_TYPE = new ArrowType.Int(8, false);
    public static final ArrowType.Int UINT16_TYPE = new ArrowType.Int(16, false);
    public static final ArrowType.Int UINT32_TYPE = new ArrowType.Int(32, false);
    public static final ArrowType.Int UINT64_TYPE = new ArrowType.Int(64, false);

    public static final ArrowType.FloatingPoint FLOAT_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    public static final ArrowType.FloatingPoint DOUBLE_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    public static final ArrowType.Decimal DECIMAL_TYPE =  ArrowType.Decimal.createDecimal(0,0,16);
    public static final ArrowType.Utf8 STRING_TYPE = new ArrowType.Utf8();
    public static final ArrowType.Binary BINARY_TYPE = new ArrowType.Binary();


    public static final ArrowType TIME_MILLI_TYPE = Types.MinorType.TIMEMILLI.getType();
    public static final ArrowType DATE_TYPE = Types.MinorType.DATEDAY.getType();
    public static final ArrowType DATETIME_MILLI_TYPE = Types.MinorType.TIMESTAMPMICRO.getType();

    public static void main(String[] args) {
        ArrowType.Int uint64Type = ArrowTypes.UINT64_TYPE;
    }
}
