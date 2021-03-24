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
package io.mycat.calcite.resultset;

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.result.*;

import java.util.TimeZone;

public class MycatValueFactory {
    public static final DefaultPropertySet DEFAULT_PROPERTY_SET = new DefaultPropertySet();
    public static final BooleanValueFactory BOOLEAN_VALUE_FACTORY = new BooleanValueFactory(DEFAULT_PROPERTY_SET);
    public static  final BigDecimalValueFactory BIG_DECIMAL_VALUE_FACTORY = new BigDecimalValueFactory(DEFAULT_PROPERTY_SET);
    public static  final BinaryStreamValueFactory BINARY_STREAM_VALUE_FACTORY = new BinaryStreamValueFactory(DEFAULT_PROPERTY_SET);
    public static  final ByteValueFactory BYTE_VALUE_FACTORY = new ByteValueFactory(DEFAULT_PROPERTY_SET);
    public static  final DoubleValueFactory DOUBLE_VALUE_FACTORY = new DoubleValueFactory(DEFAULT_PROPERTY_SET);
    public static  final FloatValueFactory FLOAT_VALUE_FACTORY = new FloatValueFactory(DEFAULT_PROPERTY_SET);
    public static  final IntegerValueFactory INTEGER_VALUE_FACTORY = new IntegerValueFactory(DEFAULT_PROPERTY_SET);
    public static  final LocalDateTimeValueFactory LOCAL_DATE_TIME_VALUE_FACTORY = new LocalDateTimeValueFactory(DEFAULT_PROPERTY_SET);
    public static  final LocalDateValueFactory LOCAL_DATE_VALUE_FACTORY = new LocalDateValueFactory(DEFAULT_PROPERTY_SET);
    public static  final LocalTimeValueFactory LOCAL_TIME_VALUE_FACTORY = new LocalTimeValueFactory(DEFAULT_PROPERTY_SET);
    public static  final LongValueFactory LONG_VALUE_FACTORY = new LongValueFactory(DEFAULT_PROPERTY_SET);
    public static  final ShortValueFactory SHORT_VALUE_FACTORY = new ShortValueFactory(DEFAULT_PROPERTY_SET);
    public static  final SqlDateValueFactory SQL_DATE_VALUE_FACTORY = new SqlDateValueFactory(DEFAULT_PROPERTY_SET,null, TimeZone.getDefault());
    public static  final SqlTimestampValueFactory SQL_TIMESTAMP_VALUE_FACTORY = new SqlTimestampValueFactory(DEFAULT_PROPERTY_SET,null,TimeZone.getDefault());
    public static  final SqlTimeValueFactory SQL_TIME_VALUE_FACTORY = new SqlTimeValueFactory(DEFAULT_PROPERTY_SET,null,TimeZone.getDefault());
    public static  final StringValueFactory STRING_VALUE_FACTORY = new StringValueFactory(DEFAULT_PROPERTY_SET);

}
