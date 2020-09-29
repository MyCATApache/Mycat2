package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ExportSetFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ExportSetFunction.class,
            "exportSet");
    public static final ExportSetFunction INSTANCE = new ExportSetFunction();
    public ExportSetFunction() {
        super("EXPORT_SET", scalarFunction);
    }

    public static String exportSet(Long bits, String on, String off, String separator, Integer number_of_bits) {
        if (bits == null || on == null || off == null || separator == null || number_of_bits == null) {
            return null;
        }
        BitSet bitSet = BitSet.valueOf(new long[]{bits});
        int length1 = bitSet.length();
        int length = Math.max(length1, number_of_bits);
        List<String> res = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            if (bitSet.get(i)) {
                res.add(on);
            } else {
                res.add(off);
            }
        }
        return String.join(separator, res);
    }

    public static String exportSet(Long bits, String on, String off, String separator) {
        return exportSet(bits, on, off, separator, 0);
    }

    public static String exportSet(Long bits, String on, String off) {
        return exportSet(bits, on, off, ",");
    }
}