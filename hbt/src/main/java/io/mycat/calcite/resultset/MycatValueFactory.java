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
