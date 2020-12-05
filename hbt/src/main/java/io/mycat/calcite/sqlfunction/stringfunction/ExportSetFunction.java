package io.mycat.calcite.sqlfunction.stringfunction;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class ExportSetFunction extends MycatSqlDefinedFunction {

    public static final ExportSetFunction INSTANCE = new ExportSetFunction();

    public ExportSetFunction() {
        super("EXPORT_SET", ReturnTypes.VARCHAR_2000,
                InferTypes.FIRST_KNOWN,
                OperandTypes.VARIADIC,
                null,
                SqlFunctionCategory.STRING);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        List<Expression> expressions = translator.translateList(call.getOperands(), nullAs);
        int size = call.getOperands().size();
        Method exportSet;
        if (size == 5) {
            exportSet = Types.lookupMethod(ExportSetFunction.class, "exportSet",
                    Long.class, String.class, String.class, String.class, String.class);
        } else if (size == 4) {
            exportSet = Types.lookupMethod(ExportSetFunction.class, "exportSet",
                    Long.class, String.class, String.class, String.class, String.class);
        } else if (size == 3) {
            exportSet = Types.lookupMethod(ExportSetFunction.class, "exportSet",
                    Long.class, String.class, String.class);
        }else {
            throw new UnsupportedOperationException(call.toString());
        }
        return Expressions.call(exportSet,expressions);
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