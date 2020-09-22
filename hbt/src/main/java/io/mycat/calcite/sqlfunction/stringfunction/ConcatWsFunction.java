package io.mycat.calcite.sqlfunction.stringfunction;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import java.util.ArrayList;

public class ConcatWsFunction extends MycatSqlDefinedFunction {
    public static final ConcatWsFunction INSTANCE = new ConcatWsFunction();

    public ConcatWsFunction() {
        super(new SqlIdentifier("concat_ws", SqlParserPos.ZERO),
                ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE, null,
                OperandTypes.repeat(SqlOperandCountRanges.from(2),
                        OperandTypes.STRING), ImmutableList.of(),
                ScalarFunctionImpl.create(ConcatWsFunction.class, "concatWs"));
    }

    public static String concatWs(String... n) {
        if (n == null || n.length == 0) {
            return null;
        }
        String seq = n[0];
        if (seq == null) return null;
        ArrayList<String> list = new ArrayList<>(n.length);
        for (int i = 1; i < n.length; i++) {
            String s = n[i];
            if (s != null){
                list.add(s);
            }
        }
        return String.join(seq,list);
    }
}