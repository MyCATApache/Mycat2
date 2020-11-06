package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

/**
 *
 * SELECT FORMAT(1234567890.09876543210, 4) AS 'Format'; 支持
 * SELECT FORMAT('1234567890.09876543210', 4) AS 'Format'; 支持
 */
public class FormatFunction extends MycatStringFunction {

    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(FormatFunction.class,
            "format");
    public static final FormatFunction INSTANCE = new FormatFunction();
    public FormatFunction() {
        super("format", scalarFunction);
    }

    public static String format(Object... args) {
        Object num = args[0];
        Integer decimal_position = (Integer) args[1];
        String locale = Locale.ROOT.getLanguage();
        if (args.length == 3) {
            locale = (String) args[2];
        }
        if (num == null || decimal_position == null) {
            return null;
        }
        NumberFormat numberFormat = DecimalFormat.getNumberInstance(new Locale(locale));
        int i = decimal_position;
        numberFormat.setMaximumFractionDigits(i < 0 ? 0 : i);
        numberFormat.setRoundingMode(RoundingMode.HALF_UP);
        BigDecimal bigDecimal = new BigDecimal(num.toString());


        return numberFormat.format(bigDecimal);
    }
}